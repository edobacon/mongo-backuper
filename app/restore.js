const fs = require('fs-extra');
const path = require('path');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const { getTimestamp, log, logSuccess, logWarning, logError, createPrompt, askQuestion } = require('./utils');
require('dotenv').config();

// Parse command line arguments
const argv = yargs(hideBin(process.argv))
  .option('truncate', {
    type: 'boolean',
    description: 'Truncate collections before importing',
    default: false
  })
  .argv;


// Function to create a temporary folder with a unique name
const createTempFolder = async (dbName) => {
  try {
    // Generate a unique hash
    const hash = crypto.randomBytes(4).toString('hex');
    const date = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
    const folderName = `${dbName}-${date}-${hash}`;
    const tempFolderPath = path.join(__dirname, '..', 'backups', folderName);

    log(`Creating temporary folder: ${tempFolderPath}`);
    await fs.ensureDir(tempFolderPath);
    logSuccess(`Created temporary folder: ${tempFolderPath}`);

    return tempFolderPath;
  } catch (error) {
    logError(`Error creating temporary folder: ${error.message}`);
    throw error;
  }
};

// Function to count JSON array entries without loading the entire file
const countJsonArrayEntries = (filePath) => {
  return new Promise((resolve, reject) => {
    let inString = false;
    let depth = 0;
    let escapeNext = false;
    let objectCount = 0;

    // Create a read stream to process the file in chunks
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });

    stream.on('data', (chunk) => {
      for (let i = 0; i < chunk.length; i++) {
        const char = chunk[i];

        // Handle escape sequences
        if (escapeNext) {
          escapeNext = false;
          continue;
        }

        if (char === '\\') {
          escapeNext = true;
          continue;
        }

        // Handle strings
        if (char === '"' && !escapeNext) {
          inString = !inString;
          continue;
        }

        if (inString) continue;

        // Handle array and object depth
        if (char === '[') {
          depth++;
        } else if (char === ']') {
          depth--;
        } else if (char === '{' && depth === 1) {
          // Count each opening brace at depth 1 (top level of the array)
          // This counts each object in the array
          objectCount++;
        }
      }
    });

    stream.on('end', () => {
      // Log the count for debugging
      log(`Counted ${objectCount} objects in the JSON array`);

      // Return the count of objects found at the top level of the array
      resolve(objectCount);
    });

    stream.on('error', (err) => {
      reject(err);
    });
  });
};

// Function to process a JSON file in chunks and write to batch files
const processJsonFileInChunks = async (sourceFilePath, tempFolderPath, collectionName, batchSize, totalDocuments) => {
  return new Promise((resolve, reject) => {
    let inString = false;
    let depth = 0;
    let escapeNext = false;
    let currentObject = '';
    let processedCount = 0;
    let batchCount = 0;
    let currentBatch = [];
    let splitFiles = [];

    // Create a read stream to process the file in chunks
    const stream = fs.createReadStream(sourceFilePath, { encoding: 'utf8' });

    // Skip the opening bracket of the array
    let skipOpeningBracket = true;

    stream.on('data', async (chunk) => {
      // Process the chunk character by character
      for (let i = 0; i < chunk.length; i++) {
        const char = chunk[i];

        // Skip the opening bracket of the array
        if (skipOpeningBracket && char === '[') {
          skipOpeningBracket = false;
          continue;
        }

        // Handle escape sequences
        if (escapeNext) {
          currentObject += char;
          escapeNext = false;
          continue;
        }

        if (char === '\\') {
          currentObject += char;
          escapeNext = true;
          continue;
        }

        // Special case: if we see a comma followed by an opening brace outside of a string,
        // and we're not currently processing an object (depth === 0), skip the comma
        if (char === ',' && !inString && depth === 0 && i + 1 < chunk.length && chunk[i + 1] === '{') {
          // Skip this comma, don't add it to currentObject
          continue;
        }

        // Add the character to the current object
        currentObject += char;

        // Handle strings
        if (char === '"' && !escapeNext) {
          inString = !inString;
          continue;
        }

        if (inString) continue;

        // Handle array and object depth
        if (char === '{') {
          depth++;
        } else if (char === '}') {
          depth--;

          // If we've reached the end of an object at depth 0, process it
          if (depth === 0) {
            // Parse the object
            try {
              const obj = JSON.parse(currentObject);
              currentBatch.push(obj);
              processedCount++;

              // Calculate progress based on the total documents counted at the beginning
              const progressPercent = totalDocuments > 0 ? Math.round((processedCount / totalDocuments) * 100) : 0;
              log(`Parsed JSON: ${progressPercent}% complete`);

              // Reset the current object
              currentObject = '';

              // If we've reached the batch size, write the batch to a file
              if (currentBatch.length >= batchSize) {
                await writeBatchToFile();
              }
            } catch (err) {
              // Try to clean up the object for parsing
              let cleanObject = currentObject;

              // If there's a comma after the object, remove it
              if (cleanObject.endsWith(',')) {
                cleanObject = cleanObject.slice(0, -1);
              }

              // Remove any whitespace at the beginning or end
              cleanObject = cleanObject.trim();

              // Try to parse the cleaned object
              try {
                const obj = JSON.parse(cleanObject);
                currentBatch.push(obj);
                processedCount++;

                // Calculate progress based on the total documents counted at the beginning
                const progressPercent = totalDocuments > 0 ? Math.round((processedCount / totalDocuments) * 100) : 0;
                log(`Parsed JSON after basic cleanup: ${progressPercent}% complete`);
              } catch (err2) {
                // If that fails, try to extract a valid JSON object
                try {
                  // Use a non-recursive approach to find a valid JSON object
                  // This avoids stack overflow with large or complex objects
                  let extractedObject = null;

                  // Check if the object is very large (> 1MB)
                  const isVeryLarge = cleanObject.length > 1024 * 1024;

                  // For very large objects, use a simpler approach to avoid performance issues
                  if (isVeryLarge) {
                    // log(`Object is very large (${(cleanObject.length / (1024 * 1024)).toFixed(2)} MB), using simple extraction`);
                    const firstBrace = cleanObject.indexOf('{');
                    const lastBrace = cleanObject.lastIndexOf('}');

                    if (firstBrace !== -1 && lastBrace !== -1 && firstBrace < lastBrace) {
                      extractedObject = cleanObject.substring(firstBrace, lastBrace + 1);
                    }
                  } else {
                    // Try to manually track braces to find a complete JSON object
                    try {
                      let braceCount = 0;
                      let startIndex = cleanObject.indexOf('{');
                      let inString = false;
                      let escapeNext = false;

                      if (startIndex !== -1) {
                        for (let i = startIndex; i < cleanObject.length; i++) {
                          const char = cleanObject[i];

                          // Handle escape sequences
                          if (escapeNext) {
                            escapeNext = false;
                            continue;
                          }

                          if (char === '\\') {
                            escapeNext = true;
                            continue;
                          }

                          // Handle strings
                          if (char === '"' && !escapeNext) {
                            inString = !inString;
                            continue;
                          }

                          // Only count braces outside of strings
                          if (!inString) {
                            if (char === '{') braceCount++;
                            else if (char === '}') braceCount--;

                            // When we've found a complete object
                            if (braceCount === 0 && i > startIndex) {
                              extractedObject = cleanObject.substring(startIndex, i + 1);
                              break;
                            }
                          }
                        }
                      }
                    } catch (bracketErr) {
                      log(`Error tracking braces: ${bracketErr.message}`);
                      // Log additional information to help diagnose the issue
                      log(`Clean object length: ${cleanObject.length}`);
                      log(`Start index: ${startIndex}`);

                      // Fall back to simple extraction if brace tracking fails
                      const firstBrace = cleanObject.indexOf('{');
                      const lastBrace = cleanObject.lastIndexOf('}');

                      if (firstBrace !== -1 && lastBrace !== -1 && firstBrace < lastBrace) {
                        extractedObject = cleanObject.substring(firstBrace, lastBrace + 1);
                        log(`Falling back to simple extraction after brace tracking error`);
                      }
                    }
                  }

                  // Log the result of failed extraction attempt
                  if (!extractedObject) {
                    log(`Failed to extract object using brace tracking`);
                  }

                  if (extractedObject) {
                    try {
                      const obj = JSON.parse(extractedObject);
                      currentBatch.push(obj);
                      processedCount++;
                      // Calculate progress based on the total documents counted at the beginning
                      const progressPercent = totalDocuments > 0 ? Math.round((processedCount / totalDocuments) * 100) : 0;
                      log(`Parsed extracted JSON: ${progressPercent}% complete`);
                    } catch (parseErr) {
                      log(`Error parsing extracted object: ${parseErr.message}`);
                      log(`Extracted object: ${extractedObject.substring(0, 100)}...`);
                    }
                  } else {
                    // Try a simpler approach - find the first { and the last }
                    const firstBrace = cleanObject.indexOf('{');
                    const lastBrace = cleanObject.lastIndexOf('}');

                    if (firstBrace !== -1 && lastBrace !== -1 && firstBrace < lastBrace) {
                      const simpleExtract = cleanObject.substring(firstBrace, lastBrace + 1);
                      try {
                        const obj = JSON.parse(simpleExtract);
                        currentBatch.push(obj);
                        processedCount++;
                        // Calculate progress based on the total documents counted at the beginning
                        const progressPercent = totalDocuments > 0 ? Math.round((processedCount / totalDocuments) * 100) : 0;
                        log(`Parsed JSON (simple extraction): ${progressPercent}% complete`);
                      } catch (simpleErr) {
                        log(`Error parsing object with simple extraction: ${simpleErr.message}`);
                        log(`Simple extracted content: ${simpleExtract.substring(0, 100)}...`);
                        log(`Original error: ${err2.message}`);
                        log(`Original object content: ${cleanObject.substring(0, 100)}...`);
                      }
                    } else {
                      // Last resort: try to fix common JSON issues
                      try {
                        // Replace unescaped quotes in values
                        let fixedJson = cleanObject.replace(/([^\\])"([^"]*[^\\])"([^:])/g, '$1\\"$2\\"$3');
                        // Remove any control characters
                        fixedJson = fixedJson.replace(/[\x00-\x1F\x7F-\x9F]/g, '');

                        // Try to parse the fixed JSON
                        const obj = JSON.parse(fixedJson);
                        currentBatch.push(obj);
                        processedCount++;
                        // Calculate progress based on the total documents counted at the beginning
                        const progressPercent = totalDocuments > 0 ? Math.round((processedCount / totalDocuments) * 100) : 0;
                        log(`Parsed JSON (aggressive cleanup): ${progressPercent}% complete`);
                      } catch (fixErr) {
                        // If all attempts fail, log the error and continue
                        log(`All parsing attempts failed: ${err2.message}`);
                        log(`Object content: ${cleanObject.substring(0, 100)}...`); // Log the first 100 chars for debugging
                      }
                    }
                  }
                } catch (err3) {
                  log(`Error in extraction process: ${err3.message}`);
                  log(`Object content: ${cleanObject.substring(0, 100)}...`); // Log the first 100 chars for debugging
                }
              }

              // Reset the current object
              currentObject = '';
            }
          }
        }
      }
    });

    // Function to write the current batch to a file
    const writeBatchToFile = async () => {
      batchCount++;
      const batchFileName = `${collectionName}_batch_${batchCount}.json`;
      const batchFilePath = path.join(tempFolderPath, batchFileName);

      try {
        await fs.writeJson(batchFilePath, currentBatch);
        splitFiles.push(batchFilePath);

        // Calculate progress based on the total documents counted at the beginning
        const progressPercent = totalDocuments > 0 ? Math.round((processedCount / totalDocuments) * 100) : 0;
        log(`Created batch file ${batchFileName} with ${currentBatch.length} documents (${progressPercent}% complete)`);

        // Reset the current batch
        currentBatch = [];
      } catch (err) {
        logError(`Error writing batch file: ${err.message}`);
        reject(err);
      }
    };

    stream.on('end', async () => {
      // Write any remaining objects to a file
      if (currentBatch.length > 0) {
        await writeBatchToFile();
      }

      logSuccess(`Split file into ${batchCount} batch files with ${processedCount} processed documents out of ${totalDocuments} total`);
      resolve({ splitFiles, totalDocuments: processedCount });
    });

    stream.on('error', (err) => {
      reject(err);
    });
  });
};

// Function to split a JSON file into smaller batches
const splitJsonFile = async (sourceFilePath, tempFolderPath, batchSize) => {
  try {
    const fileName = path.basename(sourceFilePath);
    const collectionName = path.basename(fileName, '.json');

    log(`Splitting file ${fileName} into batches of ${batchSize} documents`);

    // Get file stats to check size
    const stats = await fs.stat(sourceFilePath);
    const fileSizeMB = stats.size / (1024 * 1024);

    // For large files (>= 100MB), use the streaming approach
    if (fileSizeMB >= 100) {
      log(`File ${fileName} is large (${fileSizeMB.toFixed(2)} MB), using streaming to process`);

      // First, count the total documents
      const totalDocuments = await countJsonArrayEntries(sourceFilePath);
      log(`File ${fileName} contains approximately ${totalDocuments} documents`);

      // Process the file in chunks
      return await processJsonFileInChunks(sourceFilePath, tempFolderPath, collectionName, batchSize, totalDocuments);
    }

    // For smaller files, use the original approach
    try {
      // Read the JSON file
      const data = await fs.readJson(sourceFilePath);
      const totalDocuments = data.length;

      log(`File ${fileName} contains ${totalDocuments} documents`);

      // Calculate number of batches
      const numBatches = Math.ceil(totalDocuments / batchSize);
      const splitFiles = [];

      // Split the data into batches and write to separate files
      for (let i = 0; i < numBatches; i++) {
        const start = i * batchSize;
        const end = Math.min(start + batchSize, totalDocuments);
        const batchData = data.slice(start, end);

        const batchFileName = `${collectionName}_batch_${i + 1}.json`;
        const batchFilePath = path.join(tempFolderPath, batchFileName);

        await fs.writeJson(batchFilePath, batchData);
        splitFiles.push(batchFilePath);

        log(`Created batch file ${batchFileName} with ${batchData.length} documents (${Math.round((end / totalDocuments) * 100)}% complete)`);
      }

      logSuccess(`Split ${fileName} into ${numBatches} batch files`);
      return { splitFiles, totalDocuments };
    } catch (readError) {
      // If reading the file as JSON fails, fall back to the streaming approach
      logWarning(`Could not read ${fileName} as JSON: ${readError.message}`);
      logWarning(`Falling back to streaming approach`);

      // First, count the total documents
      const totalDocuments = await countJsonArrayEntries(sourceFilePath);
      log(`File ${fileName} contains approximately ${totalDocuments} documents`);

      // Process the file in chunks
      return await processJsonFileInChunks(sourceFilePath, tempFolderPath, collectionName, batchSize, totalDocuments);
    }
  } catch (error) {
    logError(`Error splitting JSON file ${sourceFilePath}: ${error.message}`);
    throw error;
  }
};

// Function to restore a collection from split JSON files
const restoreCollectionFromSplitFiles = async (db, splitFiles, collectionName, truncate, totalDocuments) => {
  try {
    const collection = db.collection(collectionName);

    // Truncate the collection if requested
    if (truncate) {
      log(`Truncating collection: ${collectionName}`);
      await collection.deleteMany({});
    }

    let processedDocuments = 0;

    // Process each batch file
    for (let i = 0; i < splitFiles.length; i++) {
      const batchFilePath = splitFiles[i];
      const batchFileName = path.basename(batchFilePath);

      log(`Processing batch file ${batchFileName} (${i + 1}/${splitFiles.length})`);

      // Read the batch file
      const batchData = await fs.readJson(batchFilePath);

      // Insert the data if there's any
      if (batchData.length > 0) {
        await collection.insertMany(batchData);
        processedDocuments += batchData.length;

        const progressPercent = Math.round((processedDocuments / totalDocuments) * 100);
        logSuccess(`Imported ${processedDocuments}/${totalDocuments} documents (${progressPercent}%)`);
      }
    }

    return { success: true, count: processedDocuments };
  } catch (error) {
    logError(`Error restoring collection from split files: ${error.message}`);
    return { success: false, error: error.message };
  }
};

// Function to clean up temporary folder
const cleanupTempFolder = async (tempFolderPath) => {
  try {
    log(`Cleaning up temporary folder: ${tempFolderPath}`);
    await fs.remove(tempFolderPath);
    logSuccess(`Removed temporary folder: ${tempFolderPath}`);
  } catch (error) {
    logWarning(`Error cleaning up temporary folder: ${error.message}`);
  }
};

// Function to restore a collection from a JSON file
const restoreCollectionFromJson = async (db, filePath, truncate, tempFolderPath) => {
  try {
    const fileName = path.basename(filePath);
    const collectionName = path.basename(fileName, '.json');

    // Define batch size - read from .env or use default
    const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 50000;

    log(`Processing collection: ${collectionName}`);

    // Split the JSON file into smaller batches
    const { splitFiles, totalDocuments } = await splitJsonFile(filePath, tempFolderPath, BATCH_SIZE);

    // Restore the collection from the split files
    const result = await restoreCollectionFromSplitFiles(db, splitFiles, collectionName, truncate, totalDocuments);

    return result;
  } catch (error) {
    logError(`Error restoring collection from ${filePath}: ${error.message}`);
    return { success: false, error: error.message };
  }
};

// Function to test database connection
const testDatabaseConnection = async (uri) => {
  try {
    log(`Testing connection to URI: ${uri}`);
    const client = new MongoClient(uri);
    await client.connect();
    logSuccess(`Successfully connected to MongoDB server`);
    await client.close();
    return true;
  } catch (error) {
    logError(`Error connecting to MongoDB server: ${error.message}`);
    logError('Possible solutions:');
    logError('1. Check if the MongoDB server is running');
    logError('2. Verify the connection URI is correct in restore/config.json');
    logError('3. Ensure network connectivity to the database server');
    logError('4. Check if authentication credentials are valid');
    return false;
  }
};

// Function to test database access
const testDatabaseAccess = async (uri, dbName) => {
  try {
    log(`Testing access to database: ${dbName}`);
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db(dbName);

    // Try to list collections to verify access
    await db.listCollections().toArray();

    logSuccess(`Successfully accessed database: ${dbName}`);
    await client.close();
    return true;
  } catch (error) {
    logError(`Error accessing database ${dbName}: ${error.message}`);
    return false;
  }
};

// Function to ask user to select a restore mode
const askRestoreMode = (rl) => {
  return new Promise((resolve) => {
    console.log('\n=== RESTORE MODE SELECTION ===');
    console.log('Please select a restore mode:');
    console.log('\n1. ADD CONTENT MODE');
    console.log('   - Preserves all existing data in the database');
    console.log('   - Adds new documents from the backup to existing collections');
    console.log('   - May result in duplicate documents if the same IDs exist');
    console.log('   - Recommended when you want to merge backup data with existing data');
    console.log('\n2. OVERWRITE MODE (TRUNCATE)');
    console.log('   - Deletes all existing data in each collection before importing');
    console.log('   - Replaces collections entirely with data from the backup');
    console.log('   - Ensures collections match exactly what was in the backup');
    console.log('   - Recommended when you want a clean restore or to reset collections');
    console.log('\n3. MISSING COLLECTIONS MODE');
    console.log('   - Only restores collections that don\'t exist in the database or are empty');
    console.log('   - Preserves all existing data in non-empty collections');
    console.log('   - Useful for adding missing collections without affecting existing data');
    console.log('   - Recommended when you want to fill gaps in your database structure');
    console.log('\nWARNING: Overwrite mode will delete existing data and cannot be undone!');

    rl.question('\nEnter the number of the restore mode you want to use (1, 2, or 3): ', (answer) => {
      const selection = parseInt(answer.trim());
      if (isNaN(selection) || selection < 1 || selection > 3) {
        console.log('Invalid selection. Please enter 1, 2, or 3.');
        // Ask again
        rl.close();
        const newRl = createPrompt();
        resolve(askRestoreMode(newRl));
      } else {
        let mode = 'add';
        if (selection === 2) {
          mode = 'truncate';
        } else if (selection === 3) {
          mode = 'missing';
        }

        const modeDescription = {
          'add': 'ADD CONTENT MODE (existing data will be preserved)',
          'truncate': 'OVERWRITE MODE (existing data will be deleted)',
          'missing': 'MISSING COLLECTIONS MODE (only missing or empty collections will be restored)'
        };

        console.log(`\nYou selected: ${modeDescription[mode]}`);
        resolve(mode);
        rl.close();
      }
    });
  });
};

// Function to ask user to select a backup configuration
const askBackupConfigSelection = (rl, configs) => {
  return new Promise((resolve) => {
    console.log('\n=== AVAILABLE BACKUP CONFIGURATIONS ===');
    configs.forEach((config, index) => {
      // Extract folder parts (assuming format backups/connection/db/YYYY-MM-DD)
      const folderParts = config.folder.split('/');
      const date = folderParts[folderParts.length - 1];
      const connectionFolder = folderParts.length > 1 ? folderParts[1] : 'unknown';
      const dbFolder = folderParts.length > 2 ? folderParts[2] : 'unknown';

      console.log(`${index + 1}. Reading folder: ${config.folder}`);
      console.log(`   Connection: ${connectionFolder}`);
      console.log(`   Database: ${dbFolder}`);
      console.log(`   Backup date: ${date}`);
      console.log(`   Destination URL: ${config.uri}`);
      console.log(`   Destination DB: ${config.db}`);
    });

    rl.question('\nEnter the number of the backup configuration you want to use: ', (answer) => {
      const selection = parseInt(answer.trim());
      if (isNaN(selection) || selection < 1 || selection > configs.length) {
        console.log(`Invalid selection. Please enter a number between 1 and ${configs.length}.`);
        // Ask again
        rl.close();
        const newRl = createPrompt();
        resolve(askBackupConfigSelection(newRl, configs));
      } else {
        const selectedConfig = configs[selection - 1];
        resolve(selectedConfig);
        rl.close();
      }
    });
  });
};

// Main function to restore a database
const restoreDatabase = async () => {
  let tempFolderPath = null;

  try {
    // Read restore configurations
    const configPath = path.join(__dirname, '..', 'restore', 'config.json');
    const configs = await fs.readJson(configPath);

    // Check if configs is an array
    if (!Array.isArray(configs)) {
      logWarning('restore/config.json is not an array. Converting to array format...');
      // Convert old format to new format
      const oldConfig = configs;
      const newConfigs = [oldConfig];
      await fs.writeJson(configPath, newConfigs, { spaces: 2 });
      logSuccess('Converted restore/config.json to array format');
      // Use the converted config
      const rl = createPrompt();
      const selectedConfig = await askBackupConfigSelection(rl, newConfigs);
      var { uri, db: dbName, folder } = selectedConfig;
    } else {
      // Ask user to select a configuration
      const rl = createPrompt();
      const selectedConfig = await askBackupConfigSelection(rl, configs);
      var { uri, db: dbName, folder } = selectedConfig;
    }

    // Define batch size - read from .env or use default
    const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 50000;

    // Track if all restores were successful
    let allRestoresSuccessful = true;

    log(`Restore configuration:`);
    log(`- URI: ${uri}`);
    log(`- Database: ${dbName}`);
    log(`- Folder: ${folder}`);
    log(`- Batch size: ${BATCH_SIZE} documents`);

    // Test connection to the database
    if (!(await testDatabaseConnection(uri))) {
      throw new Error(`Failed to connect to MongoDB server`);
    }

    // Test access to the database
    if (!(await testDatabaseAccess(uri, dbName))) {
      throw new Error(`Failed to access database: ${dbName}`);
    }

    // Check if the folder exists
    log(`Checking if backup folder exists: ${folder}`);
    if (!(await fs.pathExists(folder))) {
      throw new Error(`Backup folder does not exist: ${folder}`);
    }
    logSuccess(`Backup folder exists: ${folder}`);

    // Create temporary folder for split files
    tempFolderPath = await createTempFolder(dbName);

    // Ask user to select a restore mode
    const modeRl = createPrompt();
    const mode = await askRestoreMode(modeRl);
    log(`Selected restore mode: ${mode}`);

    // Get all backup files in the folder (JSON and CSV)
    const files = await fs.readdir(folder);
    const jsonFiles = files.filter(file => file.endsWith('.json'));
    const csvFiles = files.filter(file => file.endsWith('.csv'));
    const backupFiles = [...jsonFiles, ...csvFiles];

    if (backupFiles.length === 0) {
      throw new Error('No backup files (JSON or CSV) found in the backup folder');
    }

    // Log the found files by type
    if (jsonFiles.length > 0) {
      logSuccess(`Found ${jsonFiles.length} JSON files to restore`);
    }

    if (csvFiles.length > 0) {
      logWarning(`Found ${csvFiles.length} CSV files. Note: CSV restore is not yet implemented.`);
    }

    // Log total files found
    logSuccess(`Found ${backupFiles.length} total backup files`);

    // Connect to the database to check collections
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db(dbName);
    logSuccess(`Connected to database: ${db.databaseName}`);

    // Get all collections in the database
    const dbCollections = await db.listCollections().toArray();
    const dbCollectionNames = dbCollections.map(col => col.name);

    // Get collection names from backup files
    const backupCollections = backupFiles.map(file => {
      // Remove the extension (.json or .csv) to get the collection name
      const extension = path.extname(file);
      return path.basename(file, extension);
    });

    // Determine which collections to restore based on the selected mode
    const collectionsToRestore = [];

    for (const backupFile of backupFiles) {
      const extension = path.extname(backupFile);
      const collectionName = path.basename(backupFile, extension);
      const filePath = path.join(folder, backupFile);
      const isCSV = extension.toLowerCase() === '.csv';

      // Skip CSV files for now as restore is not implemented
      if (isCSV) {
        logWarning(`Skipping CSV file ${backupFile} as CSV restore is not yet implemented.`);
        continue;
      }

      // Check if collection exists in the database
      const collectionExists = dbCollectionNames.includes(collectionName);

      // Check if collection is empty (if it exists)
      let isEmpty = false;
      if (collectionExists) {
        const collection = db.collection(collectionName);
        const count = await collection.countDocuments();
        isEmpty = count === 0;
      }

      // Get document count in the backup file
      let documentCount = 0;
      try {
        // Get file stats to check size
        const stats = await fs.stat(filePath);
        const fileSizeMB = stats.size / (1024 * 1024);

        // For large files, use countJsonArrayEntries
        if (fileSizeMB >= 100) {
          documentCount = await countJsonArrayEntries(filePath);
        } else {
          // For smaller files, read the JSON
          try {
            const data = await fs.readJson(filePath);
            documentCount = data.length;
          } catch (readError) {
            // If reading fails, use countJsonArrayEntries
            documentCount = await countJsonArrayEntries(filePath);
          }
        }
      } catch (error) {
        logWarning(`Error counting documents in ${backupFile}: ${error.message}`);
      }

      // Determine if this collection should be restored based on the mode
      let shouldRestore = false;
      if (mode === 'add') {
        shouldRestore = true;
      } else if (mode === 'truncate') {
        shouldRestore = true;
      } else if (mode === 'missing') {
        shouldRestore = !collectionExists || isEmpty;
      }

      if (shouldRestore) {
        collectionsToRestore.push({
          name: collectionName,
          file: backupFile,
          exists: collectionExists,
          isEmpty: isEmpty,
          documentCount: documentCount,
          isCSV: isCSV
        });
      }
    }

    // Display which collections will be restored
    console.log('\n=== COLLECTIONS TO RESTORE ===');
    if (collectionsToRestore.length === 0) {
      console.log('No collections will be restored based on the selected mode.');
    } else {
      console.log('The following collections will be restored:');
      for (const col of collectionsToRestore) {
        const status = col.exists ? (col.isEmpty ? 'exists (empty)' : 'exists (not empty)') : 'does not exist';
        console.log(`- ${col.name}: ${col.documentCount} documents (${status})`);
      }
    }

    // Display confirmation message
    console.log('\n=== RESTORE CONFIRMATION ===');
    console.log(`You are about to restore data to database: ${dbName}`);
    console.log(`From folder: ${folder}`);
    console.log(`Collections to restore: ${collectionsToRestore.length} of ${jsonFiles.length}`);

    const modeDescriptions = {
      'add': 'ADD (data will be added to existing collections)',
      'truncate': 'OVERWRITE (existing data will be deleted)',
      'missing': 'MISSING COLLECTIONS (only missing or empty collections will be restored)'
    };

    console.log(`Mode: ${modeDescriptions[mode]}`);
    console.log(`Batch size: ${BATCH_SIZE} documents`);
    console.log('\nWARNING: This action cannot be undone. Existing data may be modified or deleted.');

    // Ask for confirmation
    const rl = createPrompt();
    const answer = await askQuestion(rl, '\nDo you want to proceed with the restore? (yes/no): ');

    if (answer.toLowerCase() !== 'yes' && answer.toLowerCase() !== 'y') {
      log('Restore process cancelled by user');
      // Clean up temporary folder
      if (tempFolderPath) {
        await cleanupTempFolder(tempFolderPath);
      }
      await client.close();
      return;
    }

    try {
      // If truncate mode is selected, check for collections that aren't in the backup
      if (mode === 'truncate') {
        // Find collections that aren't in the backup
        const collectionsNotInBackup = dbCollectionNames
          .filter(name => typeof name === 'string' && !backupCollections.includes(name) && !name.startsWith('system.'));

        if (collectionsNotInBackup.length > 0) {
          console.log('\n=== COLLECTIONS NOT IN BACKUP ===');
          console.log('The following collections exist in the database but are not in the backup:');
          console.log('These collections will be deleted if you proceed with truncate mode:');

          // Get document counts for each collection
          const collectionsWithCounts = [];
          for (const colName of collectionsNotInBackup) {
            const collection = db.collection(colName);
            const count = await collection.countDocuments();
            collectionsWithCounts.push({ name: colName, count });
            console.log(`- ${colName}: ${count} documents`);
          }

          // Ask if user wants to delete these collections
          const deleteRl = createPrompt();
          const deleteAnswer = await askQuestion(deleteRl, '\nDo you want to delete these collections? (yes/no): ');

          if (deleteAnswer.toLowerCase() === 'yes' || deleteAnswer.toLowerCase() === 'y') {
            log('Deleting collections not in backup...');
            for (const { name, count } of collectionsWithCounts) {
              log(`Deleting collection: ${name} (${count} documents)`);
              await db.collection(name).drop();
              logSuccess(`Deleted collection: ${name}`);
            }
          } else {
            log('Collections not in backup will be preserved');
          }
        }
      }

      // If no collections to restore, exit early
      if (collectionsToRestore.length === 0) {
        logWarning('No collections to restore based on the selected mode.');
        await client.close();
        if (tempFolderPath) {
          await cleanupTempFolder(tempFolderPath);
        }
        return;
      }

      log(`Collections will be processed in batches of ${BATCH_SIZE} documents`);

      // Restore each collection
      for (const collection of collectionsToRestore) {
        const filePath = path.join(folder, collection.file);

        // Skip CSV files as restore is not implemented
        if (collection.isCSV) {
          logWarning(`Skipping CSV file ${collection.file} as CSV restore is not yet implemented.`);
          continue;
        }

        // For truncate and missing modes, we need to determine if we should truncate
        // In truncate mode, always truncate
        // In missing mode, only truncate if the collection exists and is empty
        const shouldTruncate = mode === 'truncate' || (mode === 'missing' && collection.exists && collection.isEmpty);
        const result = await restoreCollectionFromJson(db, filePath, shouldTruncate, tempFolderPath);
        if (!result.success) {
          allRestoresSuccessful = false;
        }
      }

      // Close the connection
      await client.close();
      log(`Closed connection to database: ${db.databaseName}`);

      if (allRestoresSuccessful) {
        logSuccess('Restore completed successfully');
      } else {
        logWarning('Restore process completed with errors');
      }
    } catch (error) {
      logError(`Error during restore operation: ${error.message}`);

      // Close the client if it was created
      if (client) {
        try {
          await client.close();
        } catch (closeError) {
          // Ignore errors when closing an already failed connection
        }
      }
    } finally {
      // Clean up temporary folder
      if (tempFolderPath) {
        await cleanupTempFolder(tempFolderPath);
      }
    }
  } catch (error) {
    logError(`Error during restore: ${error.message}`);

    // Clean up temporary folder if it was created
    if (tempFolderPath) {
      await cleanupTempFolder(tempFolderPath);
    }

    process.exit(1);
  }
};

// Run the restore process
restoreDatabase();
