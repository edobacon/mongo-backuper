const fs = require('fs-extra');
const path = require('path');
const readline = require('readline');
const { format } = require('date-fns');
const { formatInTimeZone } = require('date-fns-tz');
require('dotenv').config();

// ANSI color codes
const colors = {
  reset: '\x1b[0m',
  black: '\x1b[30m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  brightRed: '\x1b[91m',
  brightGreen: '\x1b[92m',
  brightYellow: '\x1b[93m',
  brightBlue: '\x1b[94m',
  brightMagenta: '\x1b[95m',
  brightCyan: '\x1b[96m',
  brightWhite: '\x1b[97m'
};

// Function to get current timestamp for logging
const getTimestamp = () => {
  const timezone = process.env.TIMEZONE || 'UTC';
  return formatInTimeZone(new Date(), timezone, 'yyyy-MM-dd HH:mm:ss');
};

// Function to log messages with color based on level
const logWithColor = (message, color) => {
  console.log(`${colors.brightWhite}[${getTimestamp()}]${colors.reset} ${color}${message}${colors.reset}`);
};

// Function to log messages if logging is enabled (default: info level - blue)
const log = (message, logEnabled = true) => {
  if (logEnabled) {
    logWithColor(message, colors.blue);
  }
};

// Function for success messages (green)
const logSuccess = (message, logEnabled = true) => {
  if (logEnabled) {
    logWithColor(message, colors.green);
  }
};

// Function for warning messages (yellow)
const logWarning = (message, logEnabled = true) => {
  if (logEnabled) {
    logWithColor(message, colors.yellow);
  }
};

// Function for error messages (red)
const logError = (message) => {
  logWithColor(message, colors.red);
};

// Function to always log messages, even in silent mode (magenta)
const logAlways = (message) => {
  logWithColor(message, colors.magenta);
};

// Function to create a readline interface for user prompts
const createPrompt = () => {
  return readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
};

// Function to ask a yes/no question
const askQuestion = (rl, question) => {
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      resolve(answer.toLowerCase().trim());
      rl.close();
    });
  });
};

// Function to ask user to select a database from a list
const askDatabaseSelection = (rl, databases) => {
  return new Promise((resolve) => {
    console.log('\nAvailable databases:');
    databases.forEach((db, index) => {
      console.log(`${index + 1}. ${db.name}`);
    });

    rl.question('\nEnter the number of the database you want to backup: ', (answer) => {
      const selection = parseInt(answer.trim());
      if (isNaN(selection) || selection < 1 || selection > databases.length) {
        console.log(`Invalid selection. Please enter a number between 1 and ${databases.length}.`);
        // Ask again
        rl.close();
        const newRl = createPrompt();
        resolve(askDatabaseSelection(newRl, databases));
      } else {
        const selectedDb = databases[selection - 1];
        resolve(selectedDb.name);
        rl.close();
      }
    });
  });
};

// Function to create a backup folder name with database name and current date
const createBackupFolderName = (connectionName, selectedDbName) => {
  const now = new Date();
  const date = now.toISOString().split('T')[0]; // YYYY-MM-DD format
  return path.join('backups', connectionName, selectedDbName, date);
};

module.exports = {
  getTimestamp,
  log,
  logSuccess,
  logWarning,
  logError,
  logAlways,
  createPrompt,
  askQuestion,
  askDatabaseSelection,
  createBackupFolderName
};
