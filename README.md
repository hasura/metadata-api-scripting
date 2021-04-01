### Setup
1. clone the repo
2. run `npm install`
3. run `node main.js`

### How to use the script?
Look for the following comments - 

- `modify DB name`

   ```javascript
   let DATABASE = 'bikesDB';
   ```
   Modify the database name here


- `[1] table tracking status` - This block of code is enabled/disabled by a `boolean` flag. When this code is enabled, the script fetches the list of schema and the tables under them and display the `tracked` status according to the Hasura server.

- `[2] track tables` - This block of code is enabled/disabled by a `boolean` flag. When this code is enabled, the script will fetches the list of tables and tracks any untracked table. To add specific logic based on table name and schema for an untracked table, it can be added inside the `if (!table.is_table_tracked) { ... }` block

- `[3] untracked relationships` - This block is also enabled/disabled by a `boolean` flag. Once enabled, this fetches all the suggested relationships and stores them in the `untrackedRelations` variable indexed by the schema name. By default, the automatic tracking of relationships is disabled.

- `[4] track object relationships` and `[5] track array relationships` -  these two blocks have `boolean` flag that when enabled run the API call to track the array and object relationships respectively. Inside each of these blocks custom logic can be added for a given suggested relationship.
