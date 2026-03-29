const { MongoClient, ObjectId } = require('mongodb');
const { log, logSuccess, logWarning, logError, logProgress, logProgressEnd, createPrompt, askQuestion } = require('./utils');
const fs = require('fs-extra');
const path = require('path');
require('dotenv').config();

const SAMPLE_SIZE = parseInt(process.env.VERIFY_SAMPLE_SIZE) || 500;
const OBJECTID_REGEX = /^[0-9a-fA-F]{24}$/;
const PROGRESS_DIR = path.join(__dirname, '..', 'convert');
const PROGRESS_FILE = path.join(PROGRESS_DIR, 'progress.json');

// --- Verificacion de progreso truncado ---

const checkTruncatedProgress = async () => {
  const issues = [];

  if (!(await fs.pathExists(PROGRESS_FILE))) {
    return issues;
  }

  let progress;
  try {
    progress = await fs.readJson(PROGRESS_FILE);
  } catch (error) {
    issues.push({
      type: 'progress_corrupt',
      severity: 'error',
      message: `Archivo de progreso corrupto: ${error.message}`
    });
    return issues;
  }

  const completedKeys = new Set(progress.completed || []);
  const totalFields = progress.plan.reduce((sum, c) => sum + c.fields.length, 0);
  const completedCount = completedKeys.size;

  issues.push({
    type: 'progress_incomplete',
    severity: 'error',
    message: `Proceso de conversion incompleto (${completedCount}/${totalFields} campos). Iniciado: ${progress.startedAt}`,
    details: { progress }
  });

  // Detectar conversiones de _id que quedaron a medias (riesgo de perdida de datos)
  for (const { collectionName, fields } of progress.plan) {
    const hasId = fields.includes('_id');
    const idKey = `${collectionName}::_id`;

    if (hasId && !completedKeys.has(idKey)) {
      issues.push({
        type: 'id_conversion_interrupted',
        severity: 'critical',
        message: `Conversion de _id interrumpida en "${collectionName}". Posible perdida de documentos (delete sin insert).`,
        details: { collectionName }
      });
    }
  }

  // Campos no-_id pendientes (sin riesgo de perdida, solo conversion incompleta)
  for (const { collectionName, fields } of progress.plan) {
    const pending = fields
      .filter(f => f !== '_id')
      .filter(f => !completedKeys.has(`${collectionName}::${f}`));

    if (pending.length > 0) {
      issues.push({
        type: 'fields_pending',
        severity: 'warning',
        message: `Campos pendientes en "${collectionName}": ${pending.join(', ')}`,
        details: { collectionName, fields: pending }
      });
    }
  }

  return issues;
};

// --- Verificacion de integridad de datos ---

// Extraer todos los paths de campos candidatos a ObjectId de UN documento
// Retorna campos que son: string con formato ObjectId, ObjectId nativo, o arrays de estos
const extractObjectIdCandidates = (obj, prefix = '') => {
  const candidates = new Set();

  for (const [key, value] of Object.entries(obj)) {
    const fieldPath = prefix ? `${prefix}.${key}` : key;

    if (key === '__v' || key === 'createdAt' || key === 'updatedAt') continue;

    if (value instanceof ObjectId) {
      candidates.add(fieldPath);
    } else if (typeof value === 'string' && OBJECTID_REGEX.test(value)) {
      candidates.add(fieldPath);
    } else if (value !== null && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
      for (const nested of extractObjectIdCandidates(value, fieldPath)) {
        candidates.add(nested);
      }
    } else if (Array.isArray(value)) {
      for (const item of value) {
        if (item instanceof ObjectId || (typeof item === 'string' && OBJECTID_REGEX.test(item))) {
          candidates.add(fieldPath);
          break;
        } else if (item !== null && typeof item === 'object' && !(item instanceof Date)) {
          for (const nested of extractObjectIdCandidates(item, `${fieldPath}.$`)) {
            candidates.add(nested);
          }
          break; // Un subdocumento basta para descubrir los paths
        }
      }
    }
  }

  return candidates;
};

// Verificar una coleccion: descubrir campos con MongoDB queries directas
// No depende de analisis en memoria - consulta directamente a MongoDB por $type
const verifyCollection = async (collection, collectionName) => {
  const results = []; // { fieldPath, status: 'ok'|'string', stringCount }

  // Tomar algunos documentos para descubrir la estructura de campos
  const samples = await collection.find({}).limit(10).toArray();
  if (samples.length === 0) return results;

  // Extraer candidatos de varios documentos para cubrir variaciones
  const allCandidates = new Set();
  for (const doc of samples) {
    for (const candidate of extractObjectIdCandidates(doc)) {
      allCandidates.add(candidate);
    }
  }

  if (allCandidates.size === 0) return results;

  const candidates = Array.from(allCandidates);

  // Para cada campo candidato, consultar MongoDB directamente
  for (let i = 0; i < candidates.length; i++) {
    const fieldPath = candidates[i];

    if (candidates.length > 3) {
      logProgress(`  ${collectionName}: verificando ${fieldPath} (${i + 1}/${candidates.length})`);
    }

    // Convertir notacion interna a dot notation de MongoDB
    const mongoPath = fieldPath.replace(/\.\$\./g, '.');

    const stringCount = await collection.countDocuments({
      [mongoPath]: { $type: 'string', $regex: OBJECTID_REGEX }
    });

    results.push({
      fieldPath,
      status: stringCount === 0 ? 'ok' : 'string',
      stringCount
    });
  }

  return results;
};

// Verificar conteo de documentos contra el plan de progreso (detecta perdida por _id truncado)
const verifyDocumentCounts = async (db, progress) => {
  const issues = [];

  if (!progress || !progress.plan) return issues;

  const completedKeys = new Set(progress.completed || []);

  const planEntries = progress.plan.filter(p => p.fields.includes('_id'));
  const totalEntries = planEntries.length;

  for (let i = 0; i < totalEntries; i++) {
    const { collectionName, fields, docCount: expectedCount } = planEntries[i];
    const idKey = `${collectionName}::_id`;

    logProgress(`Verificando conteo [${i + 1}/${totalEntries}]: ${collectionName}`);

    const collection = db.collection(collectionName);
    const currentCount = await collection.estimatedDocumentCount();

    // Si la conversion de _id se completo exitosamente, el conteo deberia ser igual
    // Si se interrumpio, podrian faltar documentos
    if (currentCount < expectedCount) {
      const lost = expectedCount - currentCount;
      const wasCompleted = completedKeys.has(idKey);

      issues.push({
        type: 'document_count_mismatch',
        severity: wasCompleted ? 'warning' : 'critical',
        message: `"${collectionName}": ${currentCount} documentos actuales vs ${expectedCount} al inicio de la conversion. Diferencia: -${lost}`,
        details: {
          collectionName,
          expected: expectedCount,
          actual: currentCount,
          lost,
          idConversionCompleted: wasCompleted
        }
      });
    }
  }

  logProgressEnd();

  return issues;
};

// --- UI ---

const askRestoreConfigSelection = (rl, configs) => {
  return new Promise((resolve) => {
    console.log('\n=== BASES DE DATOS RESTAURADAS ===');
    configs.forEach((config, index) => {
      console.log(`${index + 1}. ${config.db} (${config.uri}) [${config.folder}]`);
    });

    rl.question('\nSelecciona la base de datos: ', (answer) => {
      const selection = parseInt(answer.trim());
      if (isNaN(selection) || selection < 1 || selection > configs.length) {
        console.log(`Seleccion invalida. Ingresa un numero entre 1 y ${configs.length}.`);
        rl.close();
        const newRl = createPrompt();
        resolve(askRestoreConfigSelection(newRl, configs));
      } else {
        rl.close();
        resolve(configs[selection - 1]);
      }
    });
  });
};

// --- Reporte ---

const SEVERITY_ICON = {
  critical: '\x1b[91m[CRITICO]\x1b[0m',
  error: '\x1b[31m[ERROR]\x1b[0m',
  warning: '\x1b[33m[WARN]\x1b[0m',
  info: '\x1b[34m[INFO]\x1b[0m'
};

const classifyField = (fieldPath) => {
  if (fieldPath === '_id' || fieldPath.endsWith('._id')) return '_id';
  return 'ref';
};

const printReport = (allIssues, scannedFieldsByCollection = {}) => {
  console.log('\n' + '='.repeat(60));
  console.log('=== REPORTE DE VERIFICACION ===');
  console.log('='.repeat(60));

  // Mostrar campos revisados por coleccion
  const collectionNames = Object.keys(scannedFieldsByCollection);
  if (collectionNames.length > 0) {
    console.log('\n--- CAMPOS REVISADOS POR COLECCION ---');

    for (const collectionName of collectionNames) {
      const { docCount, fields } = scannedFieldsByCollection[collectionName];
      const idFields = fields.filter(f => classifyField(f.fieldPath) === '_id');
      const refFields = fields.filter(f => classifyField(f.fieldPath) === 'ref');

      console.log(`\n  ${collectionName} (${docCount} docs):`);

      if (idFields.length > 0) {
        console.log(`    _id (${idFields.length}):`);
        for (const f of idFields) {
          const icon = f.status === 'ok' ? '\x1b[32mOK\x1b[0m' : `\x1b[31m${f.exactCount} string\x1b[0m`;
          console.log(`      - ${f.fieldPath}: ${icon}`);
        }
      }

      if (refFields.length > 0) {
        console.log(`    ref (${refFields.length}):`);
        for (const f of refFields) {
          const icon = f.status === 'ok' ? '\x1b[32mOK\x1b[0m' : `\x1b[31m${f.exactCount} string\x1b[0m`;
          console.log(`      - ${f.fieldPath}: ${icon}`);
        }
      }
    }
  }

  const criticals = allIssues.filter(i => i.severity === 'critical');
  const errors = allIssues.filter(i => i.severity === 'error');
  const warnings = allIssues.filter(i => i.severity === 'warning');
  const infos = allIssues.filter(i => i.severity === 'info');

  if (allIssues.length === 0) {
    logSuccess('\nVerificacion exitosa. No se encontraron problemas.');
    logSuccess('Todos los campos ObjectId estan correctamente tipados.');
    logSuccess('No hay procesos de conversion truncados.');
    return true;
  }

  // Criticos primero
  if (criticals.length > 0) {
    console.log('\n--- CRITICOS ---');
    for (const issue of criticals) {
      console.log(`  ${SEVERITY_ICON.critical} ${issue.message}`);
    }
  }

  // Agrupar string_objectid_remaining por coleccion con clasificacion _id/ref
  const stringOidIssues = errors.filter(i => i.type === 'string_objectid_remaining');
  const otherErrors = errors.filter(i => i.type !== 'string_objectid_remaining');

  if (stringOidIssues.length > 0) {
    console.log('\n--- CAMPOS STRING QUE DEBERIAN SER OBJECTID ---');

    // Agrupar por coleccion
    const byCollection = {};
    for (const issue of stringOidIssues) {
      const { collectionName, fieldPath, exactCount } = issue.details;
      if (!byCollection[collectionName]) {
        byCollection[collectionName] = [];
      }
      byCollection[collectionName].push({ fieldPath, exactCount });
    }

    for (const [collectionName, fields] of Object.entries(byCollection)) {
      const idFields = fields.filter(f => f.fieldPath === '_id' || f.fieldPath.endsWith('._id'));
      const refFields = fields.filter(f => f.fieldPath !== '_id' && !f.fieldPath.endsWith('._id'));

      console.log(`\n  ${collectionName}:`);

      if (idFields.length > 0) {
        console.log(`    _id (${idFields.length}):`);
        for (const f of idFields) {
          console.log(`      ${SEVERITY_ICON.error} ${f.fieldPath}: ${f.exactCount} docs`);
        }
      }

      if (refFields.length > 0) {
        console.log(`    ref (${refFields.length}):`);
        for (const f of refFields) {
          console.log(`      ${SEVERITY_ICON.error} ${f.fieldPath}: ${f.exactCount} docs`);
        }
      }
    }
  }

  if (otherErrors.length > 0) {
    console.log('\n--- OTROS ERRORES ---');
    for (const issue of otherErrors) {
      console.log(`  ${SEVERITY_ICON.error} ${issue.message}`);
    }
  }

  if (warnings.length > 0) {
    console.log('\n--- ADVERTENCIAS ---');
    for (const issue of warnings) {
      console.log(`  ${SEVERITY_ICON.warning} ${issue.message}`);
    }
  }

  if (infos.length > 0) {
    console.log('\n--- INFO ---');
    for (const issue of infos) {
      console.log(`  ${SEVERITY_ICON.info} ${issue.message}`);
    }
  }

  // Resumen
  console.log('\n--- RESUMEN ---');
  console.log(`  Criticos: ${criticals.length}`);
  console.log(`  Errores:  ${errors.length}`);
  console.log(`  Warnings: ${warnings.length}`);
  console.log(`  Info:     ${infos.length}`);

  if (criticals.length > 0) {
    logError('\nSe encontraron problemas criticos. Ejecuta "npm run convert" para completar la conversion.');
    logError('Revisa las colecciones marcadas como CRITICO por posible perdida de datos.');
  } else if (errors.length > 0) {
    logWarning('\nSe encontraron errores. Ejecuta "npm run convert" para completar la conversion.');
  } else {
    logWarning('\nSe encontraron advertencias menores.');
  }

  return false;
};

// --- Main ---

const main = async () => {
  let client;
  const allIssues = [];

  try {
    // Paso 1: Verificar progreso truncado (sin conexion)
    console.log('\n=== PASO 1: VERIFICACION DE PROGRESO ===\n');

    const progressIssues = await checkTruncatedProgress();

    if (progressIssues.length === 0) {
      logSuccess('No hay procesos de conversion pendientes');
    } else {
      allIssues.push(...progressIssues);
      for (const issue of progressIssues) {
        if (issue.severity === 'critical') {
          logError(issue.message);
        } else {
          logWarning(issue.message);
        }
      }
    }

    // Paso 2: Conectar y verificar datos
    console.log('\n=== PASO 2: VERIFICACION DE DATOS ===\n');

    const configPath = path.join(__dirname, '..', 'restore', 'config.json');
    const restoreConfigs = await fs.readJson(configPath);

    // Si hay progreso pendiente, ofrecer usar la misma conexion
    let selectedConfig;
    let progress = null;

    if (await fs.pathExists(PROGRESS_FILE)) {
      progress = await fs.readJson(PROGRESS_FILE).catch(() => null);
    }

    if (progress) {
      const rl = createPrompt();
      const answer = await askQuestion(rl,
        `Verificar la DB del progreso pendiente (${progress.dbName} en ${progress.uri})? (yes/no): `
      );

      if (answer === 'yes' || answer === 'y') {
        selectedConfig = { uri: progress.uri, db: progress.dbName };
      }
    }

    if (!selectedConfig) {
      const rl = createPrompt();
      selectedConfig = await askRestoreConfigSelection(rl, restoreConfigs);
    }

    log(`Conectando a: ${selectedConfig.uri}`);
    client = new MongoClient(selectedConfig.uri);
    await client.connect();
    logSuccess('Conexion establecida');

    const selectedDbName = selectedConfig.db;

    const db = client.db(selectedDbName);
    logSuccess(`Base de datos: ${selectedDbName}`);
    log(`Muestra por coleccion: ${SAMPLE_SIZE} documentos\n`);

    // Paso 2a: Verificar conteos si hay progreso con conversion de _id
    if (progress) {
      log('Verificando conteos de documentos contra plan de conversion...\n');
      const countIssues = await verifyDocumentCounts(db, progress);

      if (countIssues.length === 0) {
        logSuccess('Conteos de documentos consistentes con el plan');
      } else {
        allIssues.push(...countIssues);
      }
    }

    // Paso 2b: Escanear todas las colecciones buscando strings con formato ObjectId
    const collections = await db.listCollections().toArray();
    const collectionNames = collections
      .map(c => c.name)
      .filter(name => !name.startsWith('system.'));

    const totalCols = collectionNames.length;
    log(`\nEscaneando ${totalCols} colecciones...\n`);

    let cleanCollections = 0;
    let totalStringOidFields = 0;

    // Acumulador de campos revisados por coleccion (para el reporte)
    const scannedFieldsByCollection = {};

    for (let ci = 0; ci < totalCols; ci++) {
      const collectionName = collectionNames[ci];
      const collection = db.collection(collectionName);
      const docCount = await collection.estimatedDocumentCount();

      const progressLabel = `[${ci + 1}/${totalCols}]`;

      if (docCount === 0) {
        log(`${progressLabel} ${collectionName}: vacia`);
        cleanCollections++;
        continue;
      }

      logProgress(`${progressLabel} Escaneando: ${collectionName} (${docCount} docs)`);

      const fieldResults = await verifyCollection(collection, collectionName);

      if (fieldResults.length === 0) {
        cleanCollections++;
        continue;
      }

      // Registrar todos los campos revisados
      scannedFieldsByCollection[collectionName] = {
        docCount,
        fields: fieldResults.map(r => ({
          fieldPath: r.fieldPath,
          exactCount: r.stringCount,
          status: r.status
        }))
      };

      const problemFields = fieldResults.filter(r => r.status === 'string');

      if (problemFields.length === 0) {
        cleanCollections++;
        continue;
      }

      logProgressEnd();

      for (const field of problemFields) {
        totalStringOidFields++;

        allIssues.push({
          type: 'string_objectid_remaining',
          severity: 'error',
          message: `"${collectionName}.${field.fieldPath}": ${field.stringCount} documentos con string donde deberia ser ObjectId`,
          details: {
            collectionName,
            fieldPath: field.fieldPath,
            exactCount: field.stringCount,
            sampleRatio: 100
          }
        });

        logWarning(`  ${collectionName}.${field.fieldPath}: ${field.stringCount} documentos con string ObjectId`);
      }
    }

    logProgressEnd();
    log(`\n${cleanCollections}/${totalCols} colecciones sin problemas`);

    if (totalStringOidFields > 0) {
      logWarning(`${totalStringOidFields} campo(s) con strings ObjectId pendientes de conversion`);
    }

    // Reporte final
    const isClean = printReport(allIssues, scannedFieldsByCollection);

    process.exit(isClean ? 0 : 1);
  } catch (error) {
    logError(`Error: ${error.message}`);
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
      log('Conexion cerrada');
    }
  }
};

main();
