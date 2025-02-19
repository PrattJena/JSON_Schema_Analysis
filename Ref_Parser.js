const $RefParser = require('@apidevtools/json-schema-ref-parser');
const fs = require('fs').promises;
const path = require('path');

async function dereferenceJSON(filePath) {
  try {
    const schema = await $RefParser.dereference(filePath);
    const dereferencedSchemaString = JSON.stringify(schema, null, 2);
    await fs.writeFile(filePath, dereferencedSchemaString);
    console.log(`${filePath} successful.`);
  } catch (err) {
    console.error(`Error ${filePath}:`, err);
  }
}

async function processSchemas(directoryPath) {
  try {
    const items = await fs.readdir(directoryPath, { withFileTypes: true });

    for (const item of items) {
      const itemPath = path.join(directoryPath, item.name);
      if (item.isFile()) {
        await dereferenceJSON(itemPath);
      }
    }
  } catch (err) {
    console.error(`Error Reading ${directoryPath}:`);
  }
}

async function processFiles(directoryPath) {
  try {
    // Read all items in the directory
    const items = await fs.readdir(directoryPath, { withFileTypes: true });

    // Process each item
    for (const item of items) {
      const itemPath = path.join(directoryPath, item.name);

      if (item.isDirectory()) {
        // Recursively process subdirectories
        await processSchemasDirectory(itemPath);
      } else if (item.isFile() && path.extname(item.name).toLowerCase() === '.json') {
        // Process JSON files
        await dereferenceJSON(itemPath);
      }
    }
  } catch (err) {
    console.error(`Error Reading ${directoryPath}:`);
  }
}

processSchemas('schemas').then(() => console.log('All files processed successfully.'));