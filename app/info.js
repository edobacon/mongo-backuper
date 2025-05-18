const fs = require('fs-extra');
const path = require('path');
const { getTimestamp, log, logSuccess, logWarning, logError, createPrompt, askQuestion } = require('./utils');
require('dotenv').config();


// Function to list all backup folders (connection names)
const listBackupFolders = async () => {
  const backupsPath = path.join(__dirname, '..', 'backups');

  try {
    // Check if backups directory exists
    if (!(await fs.pathExists(backupsPath))) {
      logWarning('No backups directory found');
      return [];
    }

    // Get all subdirectories in the backups directory
    const items = await fs.readdir(backupsPath);
    const folders = [];

    for (const item of items) {
      const itemPath = path.join(backupsPath, item);
      const stats = await fs.stat(itemPath);

      if (stats.isDirectory()) {
        folders.push(item);
      }
    }

    return folders;
  } catch (error) {
    logError(`Error listing backup folders: ${error.message}`);
    return [];
  }
};

// Function to get the most recent backup date for a connection
const getMostRecentBackup = async (connectionName) => {
  const connectionPath = path.join(__dirname, '..', 'backups', connectionName);

  try {
    // Get all database folders in the connection directory
    const dbFolders = await fs.readdir(connectionPath);
    let mostRecentDate = null;
    let mostRecentDbName = null;
    let mostRecentBackupPath = null;

    for (const dbName of dbFolders) {
      const dbPath = path.join(connectionPath, dbName);
      const dbStats = await fs.stat(dbPath);

      if (dbStats.isDirectory()) {
        // Get all date folders in the database directory
        const dateFolders = await fs.readdir(dbPath);

        for (const dateFolder of dateFolders) {
          const datePath = path.join(dbPath, dateFolder);
          const dateStats = await fs.stat(datePath);

          if (dateStats.isDirectory()) {
            // Parse the date from the folder name
            const date = new Date(dateFolder);

            // Check if this is the most recent date
            if (!mostRecentDate || date > mostRecentDate) {
              mostRecentDate = date;
              mostRecentDbName = dbName;
              mostRecentBackupPath = datePath;
            }
          }
        }
      }
    }

    return {
      date: mostRecentDate,
      dbName: mostRecentDbName,
      path: mostRecentBackupPath
    };
  } catch (error) {
    logError(`Error getting most recent backup: ${error.message}`);
    return null;
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

// Function to get information about a backup
const getBackupInfo = async (backupPath) => {
  try {
    // Get all JSON files in the backup directory
    const files = await fs.readdir(backupPath);
    const jsonFiles = files.filter(file => file.endsWith('.json'));

    const collectionsInfo = [];

    for (const jsonFile of jsonFiles) {
      const filePath = path.join(backupPath, jsonFile);
      const stats = await fs.stat(filePath);

      // Get the file size in MB
      const fileSizeMB = stats.size / (1024 * 1024);

      // Get the collection name (remove .json extension)
      const collectionName = jsonFile.replace('.json', '');

      let entryCount = 0;

      // Use different approaches based on file size
      if (fileSizeMB < 100) {
        // For small files, use the simple approach
        try {
          const jsonData = await fs.readJson(filePath);
          entryCount = Array.isArray(jsonData) ? jsonData.length : 0;
        } catch (readError) {
          logWarning(`Could not read ${jsonFile} as JSON: ${readError.message}`);
          // Fall back to streaming approach
          entryCount = await countJsonArrayEntries(filePath);
        }
      } else {
        // For large files, use streaming approach
        log(`File ${jsonFile} is large (${fileSizeMB.toFixed(2)} MB), using streaming to count entries`);
        entryCount = await countJsonArrayEntries(filePath);
      }

      collectionsInfo.push({
        name: collectionName,
        entries: entryCount,
        sizeMB: fileSizeMB.toFixed(2)
      });
    }

    return collectionsInfo;
  } catch (error) {
    logError(`Error getting backup info: ${error.message}`);
    return [];
  }
};

// Main function to display backup information
const displayBackupInfo = async () => {
  try {
    // List all backup folders
    const backupFolders = await listBackupFolders();

    if (backupFolders.length === 0) {
      log('No backup folders found');
      return;
    }

    // Display available backup folders
    console.log('\nAvailable backup folders:');
    backupFolders.forEach((folder, index) => {
      console.log(`${index + 1}. ${folder}`);
    });

    // Ask user to select a backup folder
    const rl = createPrompt();
    const selection = await askQuestion(rl, '\nEnter the number of the backup folder you want to view: ');
    const selectionIndex = parseInt(selection) - 1;

    if (isNaN(selectionIndex) || selectionIndex < 0 || selectionIndex >= backupFolders.length) {
      logWarning('Invalid selection');
      rl.close();
      return;
    }

    const selectedFolder = backupFolders[selectionIndex];
    logSuccess(`Selected backup folder: ${selectedFolder}`);

    // Get the most recent backup for the selected folder
    const mostRecentBackup = await getMostRecentBackup(selectedFolder);

    if (!mostRecentBackup || !mostRecentBackup.path) {
      logWarning('No backups found for the selected folder');
      rl.close();
      return;
    }

    // Display information about the most recent backup
    console.log('\n=== Backup Information ===');
    console.log(`Connection: ${selectedFolder}`);
    console.log(`Database: ${mostRecentBackup.dbName}`);
    console.log(`Backup Date: ${mostRecentBackup.date.toISOString().split('T')[0]}`);
    console.log(`Backup Path: ${mostRecentBackup.path}`);

    // Get detailed information about the backup
    const collectionsInfo = await getBackupInfo(mostRecentBackup.path);

    if (collectionsInfo.length === 0) {
      logWarning('No collections found in the backup');
      rl.close();
      return;
    }

    // Display information about each collection
    console.log('\nCollections:');
    console.log('--------------------------------------------------');
    console.log('| Collection Name        | Entries  | Size (MB)  |');
    console.log('--------------------------------------------------');

    let totalEntries = 0;
    let totalSizeMB = 0;

    for (const collection of collectionsInfo) {
      // Format the collection name to fit in the table
      const nameFormatted = collection.name.padEnd(22).substring(0, 22);
      const entriesFormatted = collection.entries.toString().padEnd(8);
      const sizeFormatted = collection.sizeMB.padEnd(10);

      console.log(`| ${nameFormatted} | ${entriesFormatted} | ${sizeFormatted} |`);

      totalEntries += collection.entries;
      totalSizeMB += parseFloat(collection.sizeMB);
    }

    console.log('--------------------------------------------------');
    console.log(`| TOTAL                  | ${totalEntries.toString().padEnd(8)} | ${totalSizeMB.toFixed(2).padEnd(10)} |`);
    console.log('--------------------------------------------------');

    rl.close();
  } catch (error) {
    logError(`Error: ${error.message}`);
    process.exit(1);
  }
};

// Run the info display process
displayBackupInfo();
