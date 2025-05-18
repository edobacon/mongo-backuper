const fs = require('fs-extra');
const path = require('path');
const { MongoClient } = require('mongodb');
const archiver = require('archiver');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const { getTimestamp, log, logSuccess, logWarning, logError, logAlways, createPrompt, askQuestion, askDatabaseSelection, createBackupFolderName } = require('./utils');
require('dotenv').config();

// Get file format from environment variable or use default (json)
const FILE_FORMAT = (process.env.FILE_FORMAT || 'json').toLowerCase();

// Parse command line arguments
const argv = yargs(hideBin(process.argv))
  .option('zip', {
    type: 'boolean',
    description: 'Create a zip file instead of a folder',
    default: false
  })
  .option('log', {
    type: 'string',
    description: 'Enable or disable logging',
    default: 'on',
    choices: ['on', 'off']
  })
  .argv;

// Function to save collection data to a JSON file
const saveCollectionToJson = async (collection, outputPath) => {
  // Count documents in the collection
  const totalCount = await collection.countDocuments();
  log(`Collection ${collection.collectionName} contains ${totalCount} documents`, argv.log === 'on');

  // Check if collection is very large
  if (totalCount > 100000) {
    logWarning(`Collection ${collection.collectionName} is very large (${totalCount} documents). Backup may take a while.`, argv.log === 'on');
  }

  // Define batch size - read from .env or use default
  const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 50000;

  // Only show batch size if collection size is greater than batch size
  if (totalCount > BATCH_SIZE) {
    log(`Using batch size of ${BATCH_SIZE} documents`, argv.log === 'on');
  }

  // Initialize file with an opening bracket
  await fs.writeFile(outputPath, '[\n', 'utf8');

  let processedCount = 0;
  let isFirstBatch = true;

  // Process in batches
  while (processedCount < totalCount) {
    // Only show batch processing details if collection size is greater than batch size
    if (totalCount > BATCH_SIZE) {
      log(`Processing batch: ${processedCount + 1} to ${Math.min(processedCount + BATCH_SIZE, totalCount)} of ${totalCount} documents`, argv.log === 'on');
    }

    // Get batch of documents
    const batch = await collection.find({})
      .skip(processedCount)
      .limit(BATCH_SIZE)
      .toArray();

    // Convert batch to JSON string
    let batchJson = batch.map(doc => JSON.stringify(doc, null, 2)).join(',\n  ');

    // Add comma after previous batch if not first batch
    if (!isFirstBatch) {
      batchJson = ',\n  ' + batchJson;
    } else {
      batchJson = '  ' + batchJson;
      isFirstBatch = false;
    }

    // Append batch to file
    await fs.appendFile(outputPath, batchJson, 'utf8');

    // Update processed count
    processedCount += batch.length;

    // Log progress
    const progressPercent = Math.round((processedCount / totalCount) * 100);
    logSuccess(`Backed up ${processedCount}/${totalCount} documents (${progressPercent}%)`, argv.log === 'on');

    // Break if batch is smaller than batch size (last batch)
    if (batch.length < BATCH_SIZE) break;
  }

  // Close the JSON array
  await fs.appendFile(outputPath, '\n]', 'utf8');

  return totalCount;
};

// Function to save collection data to a CSV file
const saveCollectionToCsv = async (collection, outputPath) => {
  // Count documents in the collection
  const totalCount = await collection.countDocuments();
  log(`Collection ${collection.collectionName} contains ${totalCount} documents`, argv.log === 'on');

  // Check if collection is very large
  if (totalCount > 100000) {
    logWarning(`Collection ${collection.collectionName} is very large (${totalCount} documents). Backup may take a while.`, argv.log === 'on');
  }

  // Define batch size - read from .env or use default
  const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 50000;

  // Only show batch size if collection size is greater than batch size
  if (totalCount > BATCH_SIZE) {
    log(`Using batch size of ${BATCH_SIZE} documents`, argv.log === 'on');
  }

  let processedCount = 0;
  let isFirstBatch = true;
  let headers = [];

  // Process in batches
  while (processedCount < totalCount) {
    // Only show batch processing details if collection size is greater than batch size
    if (totalCount > BATCH_SIZE) {
      log(`Processing batch: ${processedCount + 1} to ${Math.min(processedCount + BATCH_SIZE, totalCount)} of ${totalCount} documents`, argv.log === 'on');
    }

    // Get batch of documents
    const batch = await collection.find({})
      .skip(processedCount)
      .limit(BATCH_SIZE)
      .toArray();

    // If this is the first batch, extract headers from the first document
    if (isFirstBatch && batch.length > 0) {
      // Get all unique keys from all documents in the first batch
      const allKeys = new Set();
      batch.forEach(doc => {
        Object.keys(doc).forEach(key => allKeys.add(key));
      });
      headers = Array.from(allKeys);

      // Write headers to file
      const headerLine = headers.map(header => `"${header}"`).join(',');
      await fs.writeFile(outputPath, headerLine + '\n', 'utf8');

      isFirstBatch = false;
    }

    // Convert batch to CSV rows
    let csvRows = '';
    for (const doc of batch) {
      const row = headers.map(header => {
        const value = doc[header];
        if (value === undefined || value === null) {
          return '';
        } else if (typeof value === 'object') {
          // Convert objects to JSON strings
          return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
        } else {
          // Escape quotes in string values
          return `"${String(value).replace(/"/g, '""')}"`;
        }
      }).join(',');
      csvRows += row + '\n';
    }

    // Append batch to file
    await fs.appendFile(outputPath, csvRows, 'utf8');

    // Update processed count
    processedCount += batch.length;

    // Log progress
    const progressPercent = Math.round((processedCount / totalCount) * 100);
    logSuccess(`Backed up ${processedCount}/${totalCount} documents (${progressPercent}%)`, argv.log === 'on');

    // Break if batch is smaller than batch size (last batch)
    if (batch.length < BATCH_SIZE) break;
  }

  return totalCount;
};

// Function to create a zip file from a folder
const createZipFromFolder = (folderPath, zipPath) => {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip', {
      zlib: { level: 9 } // Maximum compression
    });

    output.on('close', () => {
      resolve();
    });

    archive.on('error', (err) => {
      reject(err);
    });

    archive.pipe(output);
    archive.directory(folderPath, false);
    archive.finalize();
  });
};

// Function to ask user to select a connection from the list
const askConnectionSelection = (rl, connections) => {
  return new Promise((resolve) => {
    console.log('\nAvailable connections:');
    connections.forEach((connection, index) => {
      console.log(`${index + 1}. ${connection.name}`);
    });

    rl.question('\nEnter the number of the connection you want to use: ', (answer) => {
      const selection = parseInt(answer.trim());
      if (isNaN(selection) || selection < 1 || selection > connections.length) {
        console.log(`Invalid selection. Please enter a number between 1 and ${connections.length}.`);
        // Ask again
        rl.close();
        const newRl = createPrompt();
        resolve(askConnectionSelection(newRl, connections));
      } else {
        const selectedConnection = connections[selection - 1];
        resolve(selectedConnection);
        rl.close();
      }
    });
  });
};

// Main function to backup a single database
const backupSingleDatabase = async () => {
  try {
    // Log the start of the backup process, even in silent mode
    logAlways('Starting single database backup process');

    // Log the batch size that will be used
    const batchSize = parseInt(process.env.BATCH_SIZE) || 50000;
    log(`Batch size for document processing: ${batchSize}`, argv.log === 'on');

    // Read connections from config file
    const connectionsPath = path.join(__dirname, '..', 'config', 'connections.json');
    const connections = await fs.readJson(connectionsPath);

    if (connections.length === 0) {
      logAlways('No connections found in config file');
      return;
    }

    // Ask user to select a connection
    const rl = createPrompt();
    const selectedConnection = await askConnectionSelection(rl, connections);
    log(`Selected connection: ${selectedConnection.name}`, argv.log === 'on');

    // Connect to the database
    const { name, uri } = selectedConnection;
    const client = new MongoClient(uri);

    try {
      log(`Attempting to connect to database: ${name}...`, argv.log === 'on');
      await client.connect();
      logSuccess(`Successfully connected to database: ${name}`, argv.log === 'on');

      // List all available databases
      const adminDb = client.db().admin();
      const dbInfo = await adminDb.listDatabases();
      log(`Available databases on the server:`, argv.log === 'on');
      dbInfo.databases.forEach(database => {
        log(`- ${database.name}`, argv.log === 'on');
      });

      // Ask user to select a database
      const dbRl = createPrompt();
      const selectedDbName = await askDatabaseSelection(dbRl, dbInfo.databases);
      log(`Selected database: ${selectedDbName}`, argv.log === 'on');

      const db = client.db(selectedDbName);

      // Get all collections and display them
      const collections = await db.listCollections().toArray();
      log(`Available collections in database ${selectedDbName}:`, argv.log === 'on');
      collections.forEach(collection => {
        log(`- ${collection.name}`, argv.log === 'on');
      });

      // Create backup folder using the connection name and selected database name
      const backupFolder = createBackupFolderName(name, selectedDbName);

      // Check if the backup folder already exists
      if (await fs.pathExists(backupFolder)) {
        logWarning(`Backup folder already exists: ${backupFolder}`, argv.log === 'on');

        // Ask for confirmation to delete the existing folder
        const confirmRl = createPrompt();
        const answer = await askQuestion(confirmRl, `The backup folder ${backupFolder} already exists. Do you want to delete it and continue? (y/n): `);

        if (answer === 'y' || answer === 'yes') {
          log(`Deleting existing backup folder: ${backupFolder}`, argv.log === 'on');
          await fs.remove(backupFolder);
          await fs.ensureDir(backupFolder);
          logSuccess(`Created new backup folder: ${backupFolder}`, argv.log === 'on');
        } else {
          logWarning(`Backup process terminated by user. Existing folder was not deleted.`, argv.log === 'on');
          await client.close();
          return;
        }
      } else {
        await fs.ensureDir(backupFolder);
        logSuccess(`Created backup folder: ${backupFolder}`, argv.log === 'on');
      }

      log(`${collections.length} collections found.`, argv.log === 'on')
      // Backup each collection
      for (const collectionInfo of collections) {
        const collectionName = collectionInfo.name;
        const collection = db.collection(collectionName);

        // Determine file extension based on format
        const fileExtension = FILE_FORMAT === 'csv' ? 'csv' : 'json';
        const outputPath = path.join(backupFolder, `${collectionName}.${fileExtension}`);

        // Log the file format being used
        log(`Using ${FILE_FORMAT.toUpperCase()} format for backup`, argv.log === 'on');

        // Save collection based on format
        let count;
        if (FILE_FORMAT === 'csv') {
          count = await saveCollectionToCsv(collection, outputPath);
        } else {
          count = await saveCollectionToJson(collection, outputPath);
        }

        logSuccess(`Saved collection: ${collectionName} (${count} documents) in ${fileExtension.toUpperCase()} format`, argv.log === 'on');
      }

      // Close the connection
      await client.close();
      log(`Closed connection to database: ${selectedDbName}`, argv.log === 'on');

      // Create zip file if requested
      if (argv.zip) {
        const zipPath = `${backupFolder}.zip`;
        await createZipFromFolder(backupFolder, zipPath);
        logSuccess(`Created zip file: ${zipPath}`, argv.log === 'on');

        // Remove the folder after zipping
        await fs.remove(backupFolder);
        log(`Removed folder: ${backupFolder}`, argv.log === 'on');
      }

      logSuccess('Backup completed successfully', argv.log === 'on');
      logAlways('Backup process completed successfully');
    } catch (error) {
      logError(`Error connecting to database ${name}: ${error.message}`);
      logError('Possible solutions:');
      logError('1. Check if the MongoDB server is running');
      logError('2. Verify the connection URI is correct in config/connections.json');
      logError('3. Ensure network connectivity to the database server');
      logError('4. Check if authentication credentials are valid');

      // Close the client if it was created
      if (client) {
        try {
          await client.close();
        } catch (closeError) {
          // Ignore errors when closing an already failed connection
        }
      }

      logAlways('Backup process completed with errors');
    }
  } catch (error) {
    logError(`Error during backup: ${error.message}`);
    process.exit(1);
  }
};

// Run the backup process
backupSingleDatabase();
