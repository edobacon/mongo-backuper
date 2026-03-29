const { MongoClient, ObjectId } = require('mongodb');
const { getTimestamp, log, logSuccess, logWarning, logError, logProgress, logProgressEnd, createPrompt, askQuestion } = require('./utils');
const fs = require('fs-extra');
const path = require('path');
require('dotenv').config();

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 5000;
const OBJECTID_REGEX = /^[0-9a-fA-F]{24}$/;
const PROGRESS_DIR = path.join(__dirname, '..', 'convert');
const PROGRESS_FILE = path.join(PROGRESS_DIR, 'progress.json');

// --- Progreso persistente ---

const loadProgress = async () => {
  try {
    if (await fs.pathExists(PROGRESS_FILE)) {
      return await fs.readJson(PROGRESS_FILE);
    }
  } catch (error) {
    logWarning(`Error leyendo archivo de progreso: ${error.message}`);
  }
  return null;
};

const saveProgress = async (progress) => {
  await fs.ensureDir(PROGRESS_DIR);
  await fs.writeJson(PROGRESS_FILE, progress, { spaces: 2 });
};

const clearProgress = async () => {
  try {
    if (await fs.pathExists(PROGRESS_FILE)) {
      await fs.remove(PROGRESS_FILE);
      log('Archivo de progreso eliminado');
    }
  } catch (error) {
    logWarning(`Error eliminando archivo de progreso: ${error.message}`);
  }
};

// Generar clave unica para un campo en una coleccion
const fieldKey = (collectionName, fieldPath) => `${collectionName}::${fieldPath}`;

// --- Deteccion ---

// Extraer paths de campos candidatos a ObjectId de UN documento
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
          break;
        }
      }
    }
  }

  return candidates;
};

// Detectar campos que necesitan conversion: descubre estructura y consulta MongoDB directamente
const detectStringObjectIdFields = async (collection) => {
  // Tomar algunos documentos para descubrir la estructura
  const samples = await collection.find({}).limit(10).toArray();

  if (samples.length === 0) return [];

  // Extraer candidatos de varios documentos para cubrir variaciones de esquema
  const allCandidates = new Set();
  for (const doc of samples) {
    for (const candidate of extractObjectIdCandidates(doc)) {
      allCandidates.add(candidate);
    }
  }

  if (allCandidates.size === 0) return [];

  const candidates = Array.from(allCandidates);
  const fieldsToConvert = [];

  // Para cada campo candidato, consultar MongoDB directamente por documentos con string
  for (let i = 0; i < candidates.length; i++) {
    const fieldPath = candidates[i];

    if (candidates.length > 3) {
      logProgress(`      Verificando: ${fieldPath} (${i + 1}/${candidates.length})`);
    }

    const mongoPath = fieldPath.replace(/\.\$\./g, '.');
    const stringCount = await collection.countDocuments({
      [mongoPath]: { $type: 'string', $regex: OBJECTID_REGEX }
    });

    if (stringCount > 0) {
      fieldsToConvert.push(fieldPath);
    }
  }

  return fieldsToConvert;
};

// --- Conversion: todos los campos no-_id de una coleccion en una sola pasada ---

const convertAllFields = async (collection, collectionName, fields) => {
  const simpleFields = fields.filter(f => !f.includes('.$'));
  const arraySubdocFields = fields.filter(f => f.includes('.$'));

  // Construir filtro $or para encontrar docs que tengan CUALQUIER campo como string
  const orConditions = [];
  for (const field of simpleFields) {
    orConditions.push({ [field]: { $type: 'string', $regex: OBJECTID_REGEX } });
  }
  for (const field of arraySubdocFields) {
    const mongoPath = field.replace(/\.\$\./g, '.');
    orConditions.push({ [mongoPath]: { $type: 'string', $regex: OBJECTID_REGEX } });
  }

  if (orConditions.length === 0) return 0;

  const filter = orConditions.length === 1 ? orConditions[0] : { $or: orConditions };
  const totalToConvert = await collection.countDocuments(filter);

  if (totalToConvert === 0) {
    log(`  No hay documentos que convertir`);
    return 0;
  }

  log(`  ${totalToConvert} documentos a convertir (${fields.length} campos simultaneos)`);

  let converted = 0;

  while (true) {
    const docs = await collection.find(filter).limit(BATCH_SIZE).toArray();
    if (docs.length === 0) break;

    const bulkOps = [];

    for (let di = 0; di < docs.length; di++) {
      const doc = docs[di];
      const $set = {};

      // Campos simples (dot notation directo o arrays de strings)
      for (const fieldPath of simpleFields) {
        const value = getNestedValue(doc, fieldPath);

        if (typeof value === 'string' && OBJECTID_REGEX.test(value)) {
          $set[fieldPath] = new ObjectId(value);
        } else if (Array.isArray(value)) {
          const hasStringOid = value.some(item => typeof item === 'string' && OBJECTID_REGEX.test(item));
          if (hasStringOid) {
            $set[fieldPath] = value.map(item =>
              typeof item === 'string' && OBJECTID_REGEX.test(item) ? new ObjectId(item) : item
            );
          }
        }
      }

      // Campos en subdocumentos de arrays
      for (const fieldPath of arraySubdocFields) {
        const { arrayField, subField } = parseArraySubdocPath(fieldPath);
        const arrayValue = getNestedValue(doc, arrayField);
        if (!Array.isArray(arrayValue)) continue;

        // Verificar si algun subdocumento tiene el campo como string
        const mongoPath = fieldPath.replace(/\.\$\./g, '.');
        const docValue = getNestedValue(doc, mongoPath.split('.')[0]);
        const needsConversion = arrayValue.some(item => {
          if (item === null || typeof item !== 'object') return false;
          const val = getNestedValue(item, subField);
          return typeof val === 'string' && OBJECTID_REGEX.test(val);
        });

        if (needsConversion) {
          const updatedArray = arrayValue.map(item => {
            if (item === null || typeof item !== 'object') return item;
            return convertNestedField(item, subField);
          });
          $set[arrayField] = updatedArray;
        }
      }

      if (Object.keys($set).length > 0) {
        bulkOps.push({
          updateOne: {
            filter: { _id: doc._id },
            update: { $set }
          }
        });
      }

      // Progreso por documento para colecciones con docs pesados
      if (docs.length <= 100 && arraySubdocFields.length > 0) {
        logProgress(`  ${collectionName}: doc ${converted + di + 1}/${totalToConvert}`);
      }
    }

    if (bulkOps.length === 0) break;

    await collection.bulkWrite(bulkOps, { ordered: false });
    converted += bulkOps.length;

    const percent = Math.round((converted / totalToConvert) * 100);
    logProgress(`  ${collectionName}: ${converted}/${totalToConvert} docs (${percent}%)`);
  }

  logProgressEnd();
  logSuccess(`  ${converted} documentos convertidos (${fields.length} campos)`);
  return converted;
};

// --- Conversion: _id (requiere delete + insert) ---

const convertIdField = async (collection) => {
  const filter = { _id: { $type: 'string', $regex: OBJECTID_REGEX } };
  const totalToConvert = await collection.countDocuments(filter);

  if (totalToConvert === 0) {
    return 0;
  }

  log(`  Convirtiendo _id: ${totalToConvert} documentos (requiere recrear documentos)`);

  let converted = 0;

  while (true) {
    const docs = await collection.find(filter).limit(BATCH_SIZE).toArray();
    if (docs.length === 0) break;

    const bulkOps = [];

    for (const doc of docs) {
      const oldId = doc._id;
      const newId = new ObjectId(oldId);

      bulkOps.push({ deleteOne: { filter: { _id: oldId } } });
      bulkOps.push({ insertOne: { document: { ...doc, _id: newId } } });
    }

    if (bulkOps.length === 0) break;

    await collection.bulkWrite(bulkOps, { ordered: true });
    converted += docs.length;

    const percent = Math.round((converted / totalToConvert) * 100);
    logProgress(`    _id: ${converted}/${totalToConvert} (${percent}%)`);
  }

  logProgressEnd();
  logSuccess(`    _id: ${converted} documentos convertidos`);
  return converted;
};

// --- Utilidades de navegacion de campos ---

const getNestedValue = (obj, fieldPath) => {
  const parts = fieldPath.split('.');
  let current = obj;

  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = current[part];
  }

  return current;
};

const setNestedValue = (obj, fieldPath, value) => {
  const parts = fieldPath.split('.');
  let current = obj;

  for (let i = 0; i < parts.length - 1; i++) {
    if (current[parts[i]] === null || current[parts[i]] === undefined) return obj;
    current[parts[i]] = { ...current[parts[i]] };
    current = current[parts[i]];
  }

  current[parts[parts.length - 1]] = value;
  return obj;
};

const parseArraySubdocPath = (fieldPath) => {
  const dollarIndex = fieldPath.indexOf('.$.');
  if (dollarIndex === -1) {
    return { arrayField: fieldPath, subField: '' };
  }

  return {
    arrayField: fieldPath.substring(0, dollarIndex),
    subField: fieldPath.substring(dollarIndex + 3)
  };
};

const convertNestedField = (obj, subField) => {
  if (subField.includes('.$.')) {
    const { arrayField, subField: innerSub } = parseArraySubdocPath(subField);
    const innerArray = getNestedValue(obj, arrayField);
    if (Array.isArray(innerArray)) {
      const converted = innerArray.map(item => {
        if (item === null || typeof item !== 'object') return item;
        return convertNestedField(item, innerSub);
      });
      return setNestedValue({ ...obj }, arrayField, converted);
    }
    return obj;
  }

  const value = getNestedValue(obj, subField);

  if (typeof value === 'string' && OBJECTID_REGEX.test(value)) {
    return setNestedValue({ ...obj }, subField, new ObjectId(value));
  }

  return obj;
};

// --- UI: seleccion interactiva ---

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

// --- Fase de analisis ---

const runAnalysis = async (db, collectionNames) => {
  console.log('=== FASE 1: ANALISIS DE CAMPOS ===\n');

  const conversionPlan = [];
  const total = collectionNames.length;

  for (let i = 0; i < total; i++) {
    const collectionName = collectionNames[i];
    const collection = db.collection(collectionName);
    const docCount = await collection.estimatedDocumentCount();

    const progressLabel = `[${i + 1}/${total}]`;

    if (docCount === 0) {
      log(`${progressLabel} ${collectionName}: vacia, saltando`);
      continue;
    }

    logProgress(`${progressLabel} Analizando: ${collectionName} (${docCount} docs)`);

    const fieldsToConvert = await detectStringObjectIdFields(collection);

    logProgressEnd();

    if (fieldsToConvert.length > 0) {
      conversionPlan.push({ collectionName, fields: fieldsToConvert, docCount });
      logWarning(`${progressLabel} ${collectionName} (${docCount} docs): ${fieldsToConvert.join(', ')}`);
    } else {
      log(`${progressLabel} ${collectionName} (${docCount} docs): sin campos a convertir`);
    }
  }

  return conversionPlan;
};

// --- Fase de conversion con progreso persistente ---

const runConversion = async (db, conversionPlan, progress) => {
  console.log('\n=== FASE 2: CONVERSION ===\n');

  const completedKeys = new Set(progress.completed || []);
  let grandTotal = 0;
  let skippedCount = 0;

  const totalCollections = conversionPlan.length;

  for (let ci = 0; ci < totalCollections; ci++) {
    const { collectionName, fields } = conversionPlan[ci];
    const collection = db.collection(collectionName);

    // Separar _id del resto
    const hasId = fields.includes('_id');
    const otherFields = fields.filter(f => f !== '_id');

    // Verificar si toda la coleccion ya fue procesada
    const allFieldKeys = fields.map(f => fieldKey(collectionName, f));
    const allDone = allFieldKeys.every(k => completedKeys.has(k));

    if (allDone) {
      skippedCount += fields.length;
      continue;
    }

    console.log(`\n--- [${ci + 1}/${totalCollections}] ${collectionName} ---`);

    // Filtrar campos pendientes (excluir ya completados)
    const pendingOtherFields = otherFields.filter(f => !completedKeys.has(fieldKey(collectionName, f)));
    const skippedOther = otherFields.length - pendingOtherFields.length;
    if (skippedOther > 0) {
      log(`  ${skippedOther} campo(s) ya convertidos (saltando)`);
      skippedCount += skippedOther;
    }

    // Convertir todos los campos no-_id en una sola pasada
    if (pendingOtherFields.length > 0) {
      log(`  Campos: ${pendingOtherFields.join(', ')}`);
      const converted = await convertAllFields(collection, collectionName, pendingOtherFields);
      grandTotal += converted;

      // Marcar todos como completados
      for (const field of pendingOtherFields) {
        completedKeys.add(fieldKey(collectionName, field));
      }
      progress.completed = Array.from(completedKeys);
      await saveProgress(progress);
    }

    // _id al final (requiere delete + insert, no se puede combinar)
    if (hasId) {
      const key = fieldKey(collectionName, '_id');

      if (completedKeys.has(key)) {
        log(`  _id: ya convertido (saltando)`);
        skippedCount++;
      } else {
        const converted = await convertIdField(collection);
        grandTotal += converted;

        completedKeys.add(key);
        progress.completed = Array.from(completedKeys);
        await saveProgress(progress);
      }
    }
  }

  if (skippedCount > 0) {
    log(`\n${skippedCount} campo(s) saltados (ya convertidos en ejecucion anterior)`);
  }

  return grandTotal;
};

// --- Main ---

const main = async () => {
  let client;

  try {
    const configPath = path.join(__dirname, '..', 'restore', 'config.json');
    const restoreConfigs = await fs.readJson(configPath);

    // Verificar si hay progreso pendiente
    const existingProgress = await loadProgress();

    if (existingProgress) {
      const completedCount = (existingProgress.completed || []).length;
      const totalFields = existingProgress.plan.reduce((sum, c) => sum + c.fields.length, 0);
      const pendingCount = totalFields - completedCount;

      console.log('\n=== PROGRESO PENDIENTE ENCONTRADO ===');
      console.log(`  URI: ${existingProgress.uri}`);
      console.log(`  Base de datos: ${existingProgress.dbName}`);
      console.log(`  Iniciado: ${existingProgress.startedAt}`);
      console.log(`  Ultimo update: ${existingProgress.lastUpdatedAt || existingProgress.startedAt}`);
      console.log(`  Progreso: ${completedCount}/${totalFields} campos completados, ${pendingCount} pendientes`);

      console.log('\n  Colecciones pendientes:');
      const completedKeys = new Set(existingProgress.completed || []);
      for (const { collectionName, fields } of existingProgress.plan) {
        const pending = fields.filter(f => !completedKeys.has(fieldKey(collectionName, f)));
        if (pending.length > 0) {
          console.log(`    ${collectionName}: ${pending.join(', ')}`);
        }
      }

      const rl = createPrompt();
      const answer = await askQuestion(rl, '\nResumir proceso pendiente? (yes/no/discard): ');

      if (answer === 'discard' || answer === 'd') {
        await clearProgress();
        log('Progreso descartado. Iniciando proceso nuevo.\n');
      } else if (answer === 'yes' || answer === 'y') {
        log(`Conectando a: ${existingProgress.uri}`);

        client = new MongoClient(existingProgress.uri);
        await client.connect();
        logSuccess('Conexion establecida');

        const db = client.db(existingProgress.dbName);
        logSuccess(`Base de datos: ${existingProgress.dbName}`);
        log(`Tamano de batch: ${BATCH_SIZE}\n`);

        existingProgress.lastUpdatedAt = new Date().toISOString();
        await saveProgress(existingProgress);

        const grandTotal = await runConversion(db, existingProgress.plan, existingProgress);

        const allCompleted = existingProgress.plan
          .every(({ collectionName, fields }) =>
            fields.every(f => existingProgress.completed.includes(fieldKey(collectionName, f)))
          );

        if (allCompleted) {
          await clearProgress();
          console.log('\n=== RESULTADO ===');
          logSuccess(`Conversion completada. Total de conversiones: ${grandTotal}`);
        } else {
          console.log('\n=== RESULTADO PARCIAL ===');
          logWarning(`Conversion parcial. ${grandTotal} conversiones en esta ejecucion.`);
          logWarning(`Progreso guardado. Ejecuta de nuevo para continuar.`);
        }

        return;
      } else {
        log('Proceso cancelado.');
        return;
      }
    }

    // Flujo nuevo: seleccionar DB restaurada
    const rl = createPrompt();
    const selectedConfig = await askRestoreConfigSelection(rl, restoreConfigs);

    log(`Conectando a: ${selectedConfig.uri}`);

    client = new MongoClient(selectedConfig.uri);
    await client.connect();
    logSuccess('Conexion establecida');

    const selectedDbName = selectedConfig.db;
    const db = client.db(selectedDbName);

    logSuccess(`Base de datos seleccionada: ${selectedDbName}`);

    const collections = await db.listCollections().toArray();
    const collectionNames = collections
      .map(c => c.name)
      .filter(name => !name.startsWith('system.'));

    if (collectionNames.length === 0) {
      logError('No se encontraron colecciones');
      return;
    }

    log(`Encontradas ${collectionNames.length} colecciones`);
    log(`Tamano de batch: ${BATCH_SIZE}`);
    log(`Deteccion: query directa a MongoDB por campo\n`);

    // Fase 1: Analisis
    const conversionPlan = await runAnalysis(db, collectionNames);

    if (conversionPlan.length === 0) {
      logSuccess('\nNo se encontraron campos que necesiten conversion.');
      return;
    }

    // Mostrar resumen con clasificacion de campos
    console.log('\n=== RESUMEN DE CONVERSIONES ===');
    for (const { collectionName, fields, docCount } of conversionPlan) {
      const idFields = fields.filter(f => f === '_id' || f.endsWith('._id'));
      const refFields = fields.filter(f => f !== '_id' && !f.endsWith('._id'));

      console.log(`\n  ${collectionName} (${docCount} docs):`);

      if (idFields.length > 0) {
        console.log(`    _id (${idFields.length}):`);
        for (const field of idFields) {
          console.log(`      - ${field}: string -> ObjectId`);
        }
      }

      if (refFields.length > 0) {
        console.log(`    ref (${refFields.length}):`);
        for (const field of refFields) {
          console.log(`      - ${field}: string -> ObjectId`);
        }
      }
    }

    const confirmRl = createPrompt();
    const answer = await askQuestion(confirmRl, '\nProceder con la conversion? (yes/no): ');

    if (answer !== 'yes' && answer !== 'y') {
      log('Conversion cancelada por el usuario');
      return;
    }

    // Crear progreso inicial y persistir
    const progress = {
      uri: selectedConfig.uri,
      dbName: selectedDbName,
      plan: conversionPlan,
      completed: [],
      startedAt: new Date().toISOString(),
      lastUpdatedAt: new Date().toISOString()
    };

    await saveProgress(progress);
    log('Progreso guardado en convert/progress.json');

    // Fase 2: Conversion
    const grandTotal = await runConversion(db, conversionPlan, progress);

    // Verificar completitud
    const totalFields = conversionPlan.reduce((sum, c) => sum + c.fields.length, 0);
    const allCompleted = progress.completed.length === totalFields;

    if (allCompleted) {
      await clearProgress();
      console.log('\n=== RESULTADO ===');
      logSuccess(`Conversion completada. Total de conversiones: ${grandTotal}`);
    } else {
      console.log('\n=== RESULTADO PARCIAL ===');
      logWarning(`Conversion parcial. ${grandTotal} conversiones en esta ejecucion.`);
      logWarning(`Progreso guardado. Ejecuta de nuevo para continuar.`);
    }
  } catch (error) {
    logError(`Error: ${error.message}`);
    logWarning('El progreso fue guardado. Ejecuta de nuevo para resumir.');
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
      log('Conexion cerrada');
    }
  }
};

main();
