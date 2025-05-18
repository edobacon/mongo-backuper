# MongoDB Backuper

A Node.js application for backing up and restoring MongoDB databases. This utility allows you to create JSON or CSV backups of your MongoDB collections and restore them when needed.

## Features

- **Backup MongoDB databases** to JSON or CSV files
- **Organize backups** by database name and date
- **Compress backups** into ZIP archives
- **Silent mode** for automated backups
- **Connection status** feedback before backup starts
- **Folder existence check** with user confirmation before overwriting
- **Helpful error messages** with possible solutions for connection issues
- **Timezone-aware logs** with configurable timezone for timestamps
- **Color-coded logs** for better readability
- **Restore functionality** to import data back to MongoDB
- **Truncate option** when restoring to clear collections before import
- **Setup script** to initialize the project with necessary files and folders
- **Single database backup** to backup a specific database
- **Single database restore** to restore a specific database
- **Batch processing** for handling large collections efficiently
- **Interactive restore mode selection** to choose between adding content or overwriting collections
- **Backup information** to view details about backups

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd mongo-backuper
   ```

2. Install dependencies:
   ```
   npm install
   ```

## Configuration

### Environment Variables

Create or modify the `.env` file in the root directory to configure the application:

```
TIMEZONE=America/Santiago
BATCH_SIZE=50000
FILE_FORMAT=json
```

- `TIMEZONE`: Sets the timezone for log timestamps (default: UTC). Uses IANA timezone names (e.g., 'America/New_York', 'Europe/London', 'Asia/Tokyo').
- `BATCH_SIZE`: Sets the number of documents to process in each batch when backing up or restoring large collections (default: 50000).
- `FILE_FORMAT`: Sets the format for backup files. Supported values are 'json' and 'csv' (default: 'json'). Note that CSV restore functionality is not yet implemented.

### Backup Configuration

Create or modify the `config/connections.json` file with your MongoDB connection details:

```json
[
  {
    "name": "your_database_name",
    "uri": "mongodb://username:password@hostname:port/"
  }
]
```

You can add multiple database connections to back up several databases at once.

### Restore Configuration

Create or modify the `restore/config.json` file with your restore settings:

```json
[
  {
    "uri": "mongodb://localhost:27017/destination_db",
    "db": "destination_db",
    "folder": "backups/db_name/YYYY-MM-DD"
  },
  {
    "uri": "mongodb://localhost:27017/another_db",
    "db": "another_db",
    "folder": "backups/another_db_name/YYYY-MM-DD"
  }
]
```

The restore configuration is now an array of objects, allowing you to configure multiple restore options:

- `uri`: MongoDB connection string for the destination database
- `db`: Name of the destination database
- `folder`: Path to the backup folder containing JSON files to restore

When running the restore command, you'll be prompted to select which configuration to use.

## Usage

### Setup Command

Initializes the project with necessary files and folders:

```
npm run setup
```

This command will:
- Check if the `.env` file exists, and if not, create it with `TIMEZONE=America/Santiago` and `BATCH_SIZE=50000`
- If the `.env` file exists, validate that it contains the required settings and warn if any are missing
- Check if the `config` folder exists, and if not, create it
- Check if the `config/connections.json` file exists, and if not, create it with example data
- If `connections.json` exists, validate its format and warn if it's invalid
- Check if the `restore` folder exists, and if not, create it
- Check if the `restore/config.json` file exists, and if not, create it with example data
- If `restore/config.json` exists, validate its format and warn if it's invalid
- Log the progress to the console

### Backup Commands

#### Standard Backup

Creates a folder with JSON files for each collection:

```
npm run backup
```

This command will:
- Connect to all databases specified in `config/connections.json`
- Display connection status for each database
- Check if a backup folder already exists and prompt for confirmation before overwriting
- Create a backup folder for each database with the format `backups/<database_name>/<date>`
- Count documents in each collection before backup
- Process large collections in batches according to the BATCH_SIZE setting
- Save each collection as a separate JSON file
- Log the progress to the console, showing percentage complete for each collection
- Show helpful error messages with possible solutions if connection fails

#### Zip Backup

Creates a ZIP archive instead of a folder:

```
npm run backup:zip
```

This command performs the same operations as the standard backup but compresses the JSON files into a ZIP archive and removes the original folder.

#### Silent Backup

Creates a ZIP archive without logging to the console:

```
npm run backup:silent
```

Useful for scheduled backups where you don't need console output. Even in silent mode, the start and end of the backup process will be logged to the console.

#### Single Database Backup

Backs up a single database:

```
npm run backup:single
```

This command will:
- Ask you to select a connection from the list in `config/connections.json`
- Connect to the selected database and list all available databases on the server
- Ask you to select a database to backup
- List all collections in the selected database
- Create a backup folder with the connection name and selected database name
- Check if the backup folder already exists and ask for confirmation to delete it if it does
- Count documents in each collection before backup
- Process large collections in batches according to the BATCH_SIZE setting
- Back up each collection to a JSON file, showing percentage complete for each collection
- Optionally create a ZIP file of the backup folder

### Restore Command

Restores data from JSON files to a MongoDB database:

```
npm run restore
```

This command will:
- Read the restore configurations from `restore/config.json`
- Ask you to select which configuration to use if multiple are defined
- Ask you to select a restore mode:
  - **ADD CONTENT MODE**: Preserves existing data and adds new documents (may result in duplicates)
  - **OVERWRITE MODE (TRUNCATE)**: Deletes all existing data in each collection before importing
  - **MISSING COLLECTIONS MODE**: Only restores collections that don't exist in the database or are empty
- If OVERWRITE MODE is selected, it will also ask if you want to delete collections that aren't in the backup
- Before executing the restore, it will display which collections will be restored and how many documents each one contains
- Create a temporary folder to split large JSON files into smaller batches for efficient processing
- Process documents in batches according to the BATCH_SIZE setting
- Log the progress, showing which collections are being processed and the percentage complete
- Clean up the temporary folder after the restore is complete

### Information Command

Displays information about backups:

```
npm run info
```

This command will:
- List all backup folders (connection names) in the backups directory
- Ask you to select a backup folder
- Find the most recent backup for the selected connection
- Display information about the backup, including:
  - Connection name
  - Database name
  - Backup date
  - Backup path
  - List of collections with their entry counts and file sizes
  - Total entries and total size

## Backup Structure

Backups are organized in the following structure:

```
backups/
  ├── database1/
  │   ├── 2023-04-13/
  │   │   ├── collection1.json
  │   │   ├── collection2.json
  │   │   └── ...
  │   └── 2023-04-14.zip
  ├── database2/
  │   └── ...
  └── database3/
      ├── 2023-04-15/
      │   ├── collection1.csv
      │   ├── collection2.csv
      │   └── ...
      └── ...
```

Each backup is stored in a folder named with the date of the backup (YYYY-MM-DD format). If the zip option is used, the folder is replaced with a ZIP file of the same name. The file extension (.json or .csv) depends on the FILE_FORMAT environment variable setting.

## Dependencies

- mongodb: MongoDB Node.js driver
- fs-extra: Enhanced file system methods
- archiver: ZIP file creation
- yargs: Command-line argument parsing

## License

ISC
