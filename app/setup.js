const fs = require('fs-extra');
const path = require('path');
const { getTimestamp, log, logSuccess, logWarning, logError } = require('./utils');
require('dotenv').config();

// Validation functions
const validateEnvFile = (content) => {
  if (!content.includes('TIMEZONE=')) {
    return { valid: false, message: '.env file is missing TIMEZONE setting' };
  }
  if (!content.includes('BATCH_SIZE=')) {
    return { valid: false, message: '.env file is missing BATCH_SIZE setting' };
  }
  if (!content.includes('FILE_FORMAT=')) {
    return { valid: false, message: '.env file is missing FILE_FORMAT setting' };
  }
  return { valid: true };
};

const validateConnectionsJson = (content) => {
  if (!Array.isArray(content)) {
    return { valid: false, message: 'connections.json should be an array' };
  }

  for (const connection of content) {
    if (!connection.name || typeof connection.name !== 'string') {
      return { valid: false, message: 'Each connection must have a name property of type string' };
    }
    if (!connection.db || typeof connection.db !== 'string') {
      return { valid: false, message: 'Each connection must have a db property of type string' };
    }
    if (!connection.uri || typeof connection.uri !== 'string') {
      return { valid: false, message: 'Each connection must have a uri property of type string' };
    }
  }

  return { valid: true };
};

const validateRestoreConfigJson = (content) => {
  if (!content || !Array.isArray(content)) {
    return { valid: false, message: 'restore/config.json should be an array' };
  }

  if (content.length === 0) {
    return { valid: false, message: 'restore/config.json array should not be empty' };
  }

  for (const config of content) {
    if (!config || typeof config !== 'object' || Array.isArray(config)) {
      return { valid: false, message: 'Each item in restore/config.json should be an object' };
    }

    if (!config.uri || typeof config.uri !== 'string') {
      return { valid: false, message: 'Each item in restore/config.json must have a uri property of type string' };
    }

    if (!config.db || typeof config.db !== 'string') {
      return { valid: false, message: 'Each item in restore/config.json must have a db property of type string' };
    }

    if (!config.folder || typeof config.folder !== 'string') {
      return { valid: false, message: 'Each item in restore/config.json must have a folder property of type string' };
    }
  }

  return { valid: true };
};


// Example connection.json content
const exampleConnectionsJson = [
  {
    "name": "example_database",
    "db": "example",
    "uri": "mongodb://username:password@hostname:port/"
  }
];

// Example restore config.json content
const exampleRestoreConfigJson = [
  {
    "uri": "mongodb://localhost:27017/destination_db",
    "db": "example",
    "folder": "backups/db_name/YYYY-MM-DD"
  }
];

// Function to check and create .env file
const setupEnvFile = async () => {
  const envPath = path.join(__dirname, '..', '.env');

  try {
    if (await fs.pathExists(envPath)) {
      log('.env file already exists');

      // Read and validate .env content
      const envContent = await fs.readFile(envPath, 'utf8');
      const validation = validateEnvFile(envContent);

      if (!validation.valid) {
        logWarning(`Invalid .env file format: ${validation.message}`);
        if (validation.message.includes('TIMEZONE')) {
          logWarning('Please add TIMEZONE to your .env file manually');
        } else if (validation.message.includes('BATCH_SIZE')) {
          logWarning('Please add BATCH_SIZE to your .env file manually (recommended value: 50000)');
        } else if (validation.message.includes('FILE_FORMAT')) {
          logWarning('Please add FILE_FORMAT to your .env file manually (supported values: json, csv)');
        }
      } else {
        logSuccess('.env file has valid format');
      }
    } else {
      log('.env file does not exist, creating it');
      await fs.writeFile(envPath, 'TIMEZONE=America/Santiago\nBATCH_SIZE=50000\nFILE_FORMAT=json\n', 'utf8');
      logSuccess('Created .env file with TIMEZONE=America/Santiago, BATCH_SIZE=50000, and FILE_FORMAT=json');
    }
  } catch (error) {
    logError(`Error setting up .env file: ${error.message}`);
  }
};

// Function to check and create config folder and connections.json
const setupConfigFolder = async () => {
  const configFolderPath = path.join(__dirname, '..', 'config');
  const connectionsJsonPath = path.join(configFolderPath, 'connections.json');

  try {
    // Check if config folder exists
    if (await fs.pathExists(configFolderPath)) {
      log('config folder already exists');
    } else {
      log('config folder does not exist, creating it');
      await fs.ensureDir(configFolderPath);
      logSuccess('Created config folder');
    }

    // Check if connections.json exists
    if (await fs.pathExists(connectionsJsonPath)) {
      log('connections.json already exists');

      // Read and validate connections.json content
      try {
        const connectionsContent = await fs.readJson(connectionsJsonPath);
        const validation = validateConnectionsJson(connectionsContent);

        if (!validation.valid) {
          logWarning(`Invalid connections.json format: ${validation.message}`);
          logWarning('Please correct your connections.json file manually. Example format:');
          console.log(JSON.stringify(exampleConnectionsJson, null, 2));
        } else {
          logSuccess('connections.json has valid format');
        }
      } catch (jsonError) {
        logWarning(`Error parsing connections.json: ${jsonError.message}`);
        logWarning('Please correct your connections.json file manually. Example format:');
        console.log(JSON.stringify(exampleConnectionsJson, null, 2));
      }
    } else {
      log('connections.json does not exist, creating it with example data');
      await fs.writeJson(connectionsJsonPath, exampleConnectionsJson, { spaces: 2 });
      logSuccess('Created connections.json with example data');
    }
  } catch (error) {
    logError(`Error setting up config folder: ${error.message}`);
  }
};

// Function to check and create restore folder and config.json
const setupRestoreFolder = async () => {
  const restoreFolderPath = path.join(__dirname, '..', 'restore');
  const restoreConfigJsonPath = path.join(restoreFolderPath, 'config.json');

  try {
    // Check if restore folder exists
    if (await fs.pathExists(restoreFolderPath)) {
      log('restore folder already exists');
    } else {
      log('restore folder does not exist, creating it');
      await fs.ensureDir(restoreFolderPath);
      logSuccess('Created restore folder');
    }

    // Check if config.json exists
    if (await fs.pathExists(restoreConfigJsonPath)) {
      log('restore/config.json already exists');

      // Read and validate restore/config.json content
      try {
        const restoreConfigContent = await fs.readJson(restoreConfigJsonPath);
        const validation = validateRestoreConfigJson(restoreConfigContent);

        if (!validation.valid) {
          logWarning(`Invalid restore/config.json format: ${validation.message}`);
          logWarning('Please correct your restore/config.json file manually. Example format:');
          console.log(JSON.stringify(exampleRestoreConfigJson, null, 2));
        } else {
          logSuccess('restore/config.json has valid format');
        }
      } catch (jsonError) {
        logWarning(`Error parsing restore/config.json: ${jsonError.message}`);
        logWarning('Please correct your restore/config.json file manually. Example format:');
        console.log(JSON.stringify(exampleRestoreConfigJson, null, 2));
      }
    } else {
      log('restore/config.json does not exist, creating it with example data');
      await fs.writeJson(restoreConfigJsonPath, exampleRestoreConfigJson, { spaces: 2 });
      logSuccess('Created restore/config.json with example data');
    }
  } catch (error) {
    logError(`Error setting up restore folder: ${error.message}`);
  }
};

// Main setup function
const setup = async () => {
  log('Starting setup process');

  // Setup .env file
  await setupEnvFile();

  // Setup config folder and connections.json
  await setupConfigFolder();

  // Setup restore folder and config.json
  await setupRestoreFolder();

  logSuccess('Setup process completed');
};

// Run the setup process
setup();
