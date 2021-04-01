const fetch = require("node-fetch");
const inflection = require("inflection");
var Table = require("cli-table");

//modify DB name
let DATABASE = "bikesDB";

//modify hasura url
let HASURA_URL = "http://localhost:8080";

// SQL to get all the schema & tables (+column info on each table) present in the sql server database
let fetchSQLTablesSQL = `
SELECT sch.name as table_schema,
    obj.name as table_name,
    case
        when obj.type = 'AF' then 'Aggregate function (CLR)'
        when obj.type = 'C' then 'CHECK constraint'
        when obj.type = 'D' then 'DEFAULT (constraint or stand-alone)'
        when obj.type = 'F' then 'FOREIGN KEY constraint'
        when obj.type = 'FN' then 'SQL scalar function'
        when obj.type = 'FS' then 'Assembly (CLR) scalar-function'
        when obj.type = 'FT' then 'Assembly (CLR) table-valued function'
        when obj.type = 'IF' then 'SQL inline table-valued function'
        when obj.type = 'IT' then 'Internal table'
        when obj.type = 'P' then 'SQL Stored Procedure'
        when obj.type = 'PC' then 'Assembly (CLR) stored-procedure'
        when obj.type = 'PG' then 'Plan guide'
        when obj.type = 'PK' then 'PRIMARY KEY constraint'
        when obj.type = 'R' then 'Rule (old-style, stand-alone)'
        when obj.type = 'RF' then 'Replication-filter-procedure'
        when obj.type = 'S' then 'System base table'
        when obj.type = 'SN' then 'Synonym'
        when obj.type = 'SO' then 'Sequence object'
        when obj.type = 'U' then 'TABLE'
        when obj.type = 'EC' then 'Edge constraint'
    end as table_type,
    obj.type_desc AS comment,
    JSON_QUERY([isc].json) AS columns
FROM sys.objects as obj
    INNER JOIN sys.schemas as sch ON obj.schema_id = sch.schema_id
    OUTER APPLY (
        SELECT
            a.name AS column_name,
            a.column_id AS ordinal_position,
            ad.definition AS column_default,
            a.collation_name AS collation_name,
            CASE
                WHEN a.is_nullable = 0
                OR t.is_nullable = 0
                THEN 'NO'
                ELSE 'YES'
            END AS is_nullable,
            CASE
                WHEN t.is_table_type = 1 THEN 'TABLE'
                WHEN t.is_assembly_type = 1 THEN 'ASSEMBLY'
                WHEN t.is_user_defined = 1 THEN 'USER-DEFINED'
                ELSE 'OTHER'
            END AS data_type,
            t.name AS data_type_name
        FROM
            sys.columns a
            LEFT JOIN sys.default_constraints ad ON (a.column_id = ad.parent_column_id AND a.object_id = ad.parent_object_id)
            JOIN sys.types t ON a.user_type_id = t.user_type_id
        WHERE a.column_id > 0 and a.object_id = obj.object_id
        FOR JSON path
) AS [isc](json) where obj.type_desc in ('USER_TABLE', 'VIEW')
`;

let fetchFKRelationships = `
SELECT
    fk.name AS constraint_name,
    sch1.name AS [table_schema],
    tab1.name AS [table_name],
    sch2.name AS [ref_table_schema],
    tab2.name AS [ref_table],
    (
        SELECT
            col1.name AS [column],
            col2.name AS [referenced_column]
        FROM sys.foreign_key_columns fkc
        INNER JOIN sys.columns col1
            ON col1.column_id = fkc.parent_column_id AND col1.object_id = tab1.object_id
        INNER JOIN sys.columns col2
            ON col2.column_id = fkc.referenced_column_id AND col2.object_id = tab2.object_id 
        WHERE fk.object_id = fkc.constraint_object_id
        FOR JSON PATH
    ) AS column_mapping,
    fk.delete_referential_action_desc AS [on_delete],
    fk.update_referential_action_desc AS [on_update]
FROM sys.foreign_keys fk
INNER JOIN sys.objects obj
    ON obj.object_id = fk.referenced_object_id
INNER JOIN sys.tables tab1
    ON tab1.object_id = fk.parent_object_id
INNER JOIN sys.schemas sch1
    ON tab1.schema_id = sch1.schema_id
INNER JOIN sys.tables tab2
    ON tab2.object_id = fk.referenced_object_id
INNER JOIN sys.schemas sch2
    ON tab2.schema_id = sch2.schema_id for json path;
`;

async function getListOfTables(database = DATABASE) {
  let body = {
    type: "mssql_run_sql",
    args: {
      source: database,
      sql: fetchSQLTablesSQL,
      cascade: false,
      read_only: false,
    },
  };
  let res = await fetch(`${HASURA_URL}/v2/query`, {
    method: "post",
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
  });
  res = await res.json();
  return res;
}

async function getListOfFKRelationships(database = DATABASE) {
  let body = {
    type: "mssql_run_sql",
    args: {
      source: database,
      sql: fetchFKRelationships,
      cascade: false,
      read_only: false,
    },
  };
  let res = await fetch(`${HASURA_URL}/v2/query`, {
    method: "post",
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
  });
  res = await res.json();
  return res;
}

async function getListOfTrackedTables(database = DATABASE) {
  let body = { type: "export_metadata", args: {} };
  let res = await fetch(`${HASURA_URL}/v1/metadata`, {
    method: "post",
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
  });
  res = await res.json();
  if (res.sources) {
    return res.sources.filter(
      (s) => s.name === DATABASE && s.kind === "mssql"
    )[0].tables;
  }
  return res.json();
}

async function updateMetaData(body) {
  let res = await fetch(`${HASURA_URL}/v1/metadata`, {
    method: "post",
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
  });
  return res.json();
}

const permKeys = [
  "insert_permissions",
  "update_permissions",
  "select_permissions",
  "delete_permissions",
];

const keyToPermission = {
  insert_permissions: "insert",
  update_permissions: "update",
  select_permissions: "select",
  delete_permissions: "delete",
};

const mergeDataMssql = (data, metadataTables) => {
  const result = [];
  const tables = [];
  let fkRelations = [];
  data[0].result.slice(1).forEach((row) => {
    try {
      tables.push({
        table_schema: row[0],
        table_name: row[1],
        table_type: row[2],
        comment: row[3],
        columns: JSON.parse(row[4]),
      });
      // eslint-disable-next-line no-empty
    } catch (err) {
      console.log(err);
    }
  });
  try {
    fkRelations = JSON.parse(data[1].result.slice(1).join());
    // eslint-disable-next-line no-empty
  } catch {}

  console.log(JSON.stringify(fkRelations))
  console.log(metadataTables)
  const trackedFkData = fkRelations
    .map((fk) => ({
      ...fk,
      is_table_tracked: !!metadataTables.some(
        (t) =>
          t.table.name === fk.table_name && t.table.schema === fk.table_schema
      ),
      is_ref_table_tracked: !!metadataTables.some(
        (t) =>
          t.table.name === fk.ref_table &&
          t.table.schema === fk.ref_table_schema
      ),
    }))
    .map((fk) => {
      const mapping = {};
      fk.column_mapping.forEach((cols) => {
        mapping[cols.column] = cols.referenced_column;
      });
      return {
        ...fk,
        column_mapping: mapping,
        ref_table_table_schema: fk.ref_table_schema,
      };
    });

  tables.forEach((table) => {
    const metadataTable = metadataTables?.find(
      (t) =>
        t.table.schema === table.table_schema &&
        t.table.name === table.table_name
    );

    const fkConstraints = trackedFkData.filter(
      (fk) =>
        fk.table_schema === table.table_schema &&
        fk.table_name === table.table_name
    );

    const refFkConstraints = trackedFkData.filter(
      (fk) =>
        fk.ref_table_schema === table.table_schema &&
        fk.ref_table === table.table_name &&
        fk.is_ref_table_tracked
    );

    const relationships = [];
    metadataTable?.array_relationships?.forEach((rel) => {
      relationships.push({
        rel_def: rel.using,
        rel_name: rel.name,
        table_name: table.table_name,
        table_schema: table.table_schema,
        rel_type: "array",
      });
    });

    metadataTable?.object_relationships?.forEach((rel) => {
      relationships.push({
        rel_def: rel.using,
        rel_name: rel.name,
        table_name: table.table_name,
        table_schema: table.table_schema,
        rel_type: "object",
      });
    });

    const rolePermMap = permKeys.reduce((rpm, key) => {
      if (metadataTable) {
        metadataTable[key]?.forEach((perm) => {
          rpm[perm.role] = {
            permissions: {
              ...(rpm[perm.role] && rpm[perm.role].permissions),
              [keyToPermission[key]]: perm.permission,
            },
          };
        });
      }
      return rpm;
    }, {});

    const permissions = Object.keys(rolePermMap).map((role) => ({
      role_name: role,
      permissions: rolePermMap[role].permissions,
      table_name: table.table_name,
      table_schema: table.table_schema,
    }));

    const mergedInfo = {
      table_schema: table.table_schema,
      table_name: table.table_name,
      table_type: table.table_type,
      is_table_tracked: metadataTables.some(
        (t) =>
          t.table.name === table.table_name &&
          t.table.schema === table.table_schema
      ),
      columns: table.columns,
      comment: "",
      triggers: [],
      primary_key: null,
      relationships,
      permissions,
      unique_constraints: [],
      check_constraints: [],
      foreign_key_constraints: fkConstraints,
      opp_foreign_key_constraints: refFkConstraints,
      view_info: null,
      remote_relationships: [],
      is_enum: false,
      configuration: undefined,
      computed_fields: [],
    };

    result.push(mergedInfo);
  });
  return result;
};

const sameRelCols = (currCols, existingCols) => {
  return currCols.sort().join(",") === existingCols.sort().join(",");
};

const isExistingObjRel = (currentObjRels, relCols) => {
  let _isExistingObjRel = false;

  for (let k = 0; k < currentObjRels.length; k++) {
    const objRelDef = currentObjRels[k].rel_def;

    if (objRelDef.foreign_key_constraint_on) {
      // check if this is already an existing fk relationship
      if (
        // TODO: update when multiCol fkey rels are allowed
        relCols.length === 1 &&
        objRelDef.foreign_key_constraint_on === relCols[0]
      ) {
        _isExistingObjRel = true;
        break;
      }
    } else {
      // check if this is already an existing manual relationship
      const objRelCols = Object.keys(
        objRelDef.manual_configuration.column_mapping
      );
      if (sameRelCols(objRelCols, relCols)) {
        _isExistingObjRel = true;
        break;
      }
    }
  }

  return _isExistingObjRel;
};

const isExistingArrRel = (currentArrRels, relCols, relTable) => {
  let _isExistingArrRel = false;

  for (let k = 0; k < currentArrRels.length; k++) {
    const arrRelDef = currentArrRels[k].rel_def;

    let currTable = null;
    let currRCol = null;

    if (arrRelDef.foreign_key_constraint_on) {
      // check if this is already an existing fk relationship
      currTable = arrRelDef.foreign_key_constraint_on.table;
      currRCol = [arrRelDef.foreign_key_constraint_on.column];
    } else {
      // check if this is already an existing manual relationship
      currTable = arrRelDef.manual_configuration.remote_table;
      currRCol = Object.values(arrRelDef.manual_configuration.column_mapping);
    }

    if (currTable.name === relTable && sameRelCols(currRCol, relCols)) {
      _isExistingArrRel = true;
      break;
    }
  }

  return _isExistingArrRel;
};

const suggestedRelationshipsRaw = (tableName, allSchemas, currentSchema) => {

  const objRels = [];
  const arrRels = [];

  const currentTableSchema = allSchemas.find(
    (t) => t.table_name === tableName && t.table_schema === currentSchema
  );

  const currentTableRelationships = currentTableSchema.relationships;

  const currentObjRels = currentTableRelationships.filter(
    (r) => r.rel_type === "object"
  );
  const currentArrRels = currentTableRelationships.filter(
    (r) => r.rel_type === "array"
  );

  currentTableSchema.foreign_key_constraints.forEach((fk_obj) => {
    if (!fk_obj.is_ref_table_tracked) {
      return;
    }

    const lcol = Object.keys(fk_obj.column_mapping);

    if (!isExistingObjRel(currentObjRels, lcol)) {
      objRels.push({
        lTable: fk_obj.table_name,
        lSchema: fk_obj.table_schema,
        isObjRel: true,
        name: null,
        lcol: lcol,
        rcol: lcol.map((column) => fk_obj.column_mapping[column]),
        rTable: fk_obj.ref_table,
        rSchema: fk_obj.ref_table_table_schema,
        isUnique: false,
      });
    }
  });

  currentTableSchema.opp_foreign_key_constraints.forEach((o_fk_obj) => {
    if (!o_fk_obj.is_table_tracked) {
      return;
    }

    const rcol = Object.keys(o_fk_obj.column_mapping);
    const lcol = Object.values(o_fk_obj.column_mapping);
    const rTable = o_fk_obj.table_name;

    if (o_fk_obj.is_unique) {
      // if opp foreign key is also unique, make obj rel
      if (!isExistingObjRel(currentObjRels, lcol)) {
        objRels.push({
          lTable: o_fk_obj.ref_table,
          lSchema: o_fk_obj.ref_table_table_schema,
          name: null,
          rcol: rcol,
          lcol: rcol.map((column) => o_fk_obj.column_mapping[column]),
          rTable: rTable,
          rSchema: o_fk_obj.table_schema,
          isObjRel: true,
          isUnique: true,
        });
      }
    } else {
      if (!isExistingArrRel(currentArrRels, rcol, o_fk_obj.table_name)) {
        arrRels.push({
          lTable: o_fk_obj.ref_table,
          lSchema: o_fk_obj.ref_table_table_schema,
          name: null,
          rcol: rcol,
          lcol: rcol.map((column) => o_fk_obj.column_mapping[column]),
          rTable: rTable,
          rSchema: o_fk_obj.table_schema,
          isObjRel: false,
          isUnique: false,
        });
      }
    }
  });

  const length =
    objRels.length > arrRels.length ? objRels.length : arrRels.length;
  const finalObjRel = [];
  const finalArrayRel = [];
  for (let i = 0; i < length; i++) {
    const objRel = objRels[i] ? objRels[i] : null;
    const arrRel = arrRels[i] ? arrRels[i] : null;
    if (objRel !== null) {
      finalObjRel.push(objRel);
    }
    if (arrRel !== null) {
      finalArrayRel.push(arrRel);
    }
  }

  return { objectRel: finalObjRel, arrayRel: finalArrayRel };
};

const getExistingFieldsMap = (tableSchema) => {
  const fieldMap = {};

  tableSchema.relationships.forEach((tr) => {
    fieldMap[tr.rel_name] = true;
  });

  tableSchema.columns.forEach((tc) => {
    fieldMap[tc.column_name] = true;
  });

  return fieldMap;
};

const sanitizeRelName = (arg) => arg.trim();

const fallBackRelName = (relMeta, existingFields, iterNumber = 0) => {
  let relName;
  const targetTable = sanitizeRelName(relMeta.rTable);
  if (relMeta.isObjRel) {
    const objLCol = sanitizeRelName(relMeta.lcol.join("_"));
    relName = `${inflection.singularize(targetTable)}_by_${objLCol}${
      iterNumber ? "_" + iterNumber : ""
    }`;
  } else {
    const arrRCol = sanitizeRelName(relMeta.rcol.join("_"));
    relName = `${inflection.pluralize(targetTable)}_by_${arrRCol}${
      iterNumber ? "_" + iterNumber : ""
    }`;
  }
  relName = inflection.camelize(relName, true);
  /*
   * Recurse until a unique relationship name is found and keep prefixing an integer at the end to fix collision
   * */
  return relName in existingFields
    ? fallBackRelName(relMeta, existingFields, ++iterNumber)
    : relName;
};

const getMetadataQuery = (type, source, args) => {
  return {
    type: `mssql_${type}`,
    args: { ...args, source },
  };
};

const getCreateArrayRelationshipQuery = (table, name, source) =>
  getMetadataQuery("create_array_relationship", source, {
    name,
    table,
    using: {},
  });

const getCreateObjectRelationshipQuery = (table, name, source) =>
  getMetadataQuery("create_object_relationship", source, {
    name,
    table,
    using: {},
  });

const formRelName = (relMeta, existingFields) => {
  try {
    let finalRelName;
    const targetTable = sanitizeRelName(relMeta.rTable);
    if (relMeta.isObjRel) {
      finalRelName = inflection.singularize(targetTable);
    } else {
      finalRelName = inflection.pluralize(targetTable);
    }

    /* Check if it is existing, fallback to guaranteed unique name */
    if (existingFields && finalRelName in existingFields) {
      finalRelName = fallBackRelName(relMeta, existingFields);
    }

    return finalRelName;
  } catch (e) {
    console.log(e);
    return "";
  }
};

const generateRelationshipsQuery = (relMeta, currentDataSource) => {
  let _upQuery;
  let _downQuery;

  if (relMeta.isObjRel) {
    _upQuery = getCreateObjectRelationshipQuery(
      {
        name: relMeta.lTable,
        schema: relMeta.lSchema,
      },
      relMeta.relName,
      currentDataSource
    );
    const columnMaps = relMeta.lcol.map((column, index) => ({
      lcol: column,
      rcol: relMeta.rcol[index],
    }));
    if (columnMaps.length === 1 && !relMeta.isUnique) {
      _upQuery.args.using = {
        foreign_key_constraint_on: relMeta.lcol[0],
      };
    } else {
      const columnReducer = (accumulator, val) => ({
        ...accumulator,
        [val.lcol]: val.rcol,
      });
      _upQuery.args.using = {
        manual_configuration: {
          remote_table: {
            name: relMeta.rTable,
            schema: relMeta.rSchema,
          },
          source: relMeta.source,
          column_mapping: columnMaps.reduce(columnReducer, {}),
        },
      };
    }

    _downQuery = "";
  } else {
    _upQuery = getCreateArrayRelationshipQuery(
      {
        name: relMeta.lTable,
        schema: relMeta.lSchema,
      },
      relMeta.relName,
      currentDataSource
    );
    const columnMaps = relMeta.rcol.map((column, index) => ({
      rcol: column,
      lcol: relMeta.lcol[index],
    }));
    if (columnMaps.length === 1) {
      _upQuery.args.using = {
        foreign_key_constraint_on: {
          table: {
            name: relMeta.rTable,
            schema: relMeta.rSchema,
          },
          column: relMeta.rcol[0],
        },
      };
    } else {
      const columnReducer = (accumulator, val) => ({
        ...accumulator,
        [val.lcol]: val.rcol,
      });
      _upQuery.args.using = {
        manual_configuration: {
          remote_table: {
            name: relMeta.rTable,
            schema: relMeta.rSchema,
          },
          source: currentDataSource,
          column_mapping: columnMaps.reduce(columnReducer, {}),
        },
      };
    }

    _downQuery = "";
  }

  return { upQuery: _upQuery, downQuery: _downQuery };
};

const getTableColumnNames = (table) => {
  return table.columns.map((c) => c.column_name);
};

function escapeTableColumns(table) {
  if (!table) return {};
  const pattern = /\W/g;
  return getTableColumnNames(table)
    .filter((col) => pattern.test(col))
    .reduce((acc, col) => {
      acc[col] = col.replace(pattern, "_");
      return acc;
    }, {});
}

async function mergeData() {
  let res1 = await getListOfTables();
  let res2 = await getListOfFKRelationships();
  let metaDataTables = await getListOfTrackedTables();
  let allSchemas = mergeDataMssql([res1, res2], metaDataTables);
  return allSchemas;
}

async function main() {
  let allSchemas = [];

  // [1] table tracking status
  if (false) {
    let display = new Table({
      head: ["schema", "table", "is is tracked"],
      colWidths: [30, 30, 30],
    });
    allSchemas = await mergeData();
    allSchemas.forEach((table) => {
      display.push([
        table.table_schema,
        table.table_name,
        table.is_table_tracked,
      ]);
    });
    console.log(display.toString());
  }

  // [2] track tables
  if (false) {
    allSchemas = await mergeData();
    let promises = [];
    allSchemas.forEach(async (table) => {
      if (!table.is_table_tracked) {
        try {
          // block where each "table tracking API call" is made
          let body = {
            type: "mssql_track_table",
            args: {
              table: {
                name: table.table_name,
                schema: table.table_schema,
              },
              source: DATABASE,
              customColumnNames: escapeTableColumns(table),
            },
          };

          let res = await updateMetaData(body);
          console.log(
            `${table.table_name} ${table.table_schema} ${JSON.stringify(res)}`
          );
        } catch (err) {
          console.log(err);
        }
      }
    });
  }

  // [3] untracked relationships
  if (false) {
    
    let res1 = await getListOfTables();
    let res2 = await getListOfFKRelationships();
    let metaDataTables = await getListOfTrackedTables();

    
    let uniqueSchemas = metaDataTables.map(t => t.table.schema).filter((item, i, ar) => ar.indexOf(item) === i);
    console.log(uniqueSchemas);

    let untrackedRelations = {};

    uniqueSchemas.forEach((currentSchema) => {
      let allSchemas = mergeDataMssql([res1, res2], metaDataTables.filter(t => t.table.schema === currentSchema));
      let currentSource = DATABASE;
      const trackedTables = allSchemas.filter(
        (table) =>
          table.is_table_tracked && table.table_schema === currentSchema
      );

      const tableRelMapping = trackedTables.map((table) => ({
        table_name: table.table_name,
        existingFields: getExistingFieldsMap(table),
        relations: suggestedRelationshipsRaw(
          table.table_name,
          allSchemas,
          currentSchema
        ),
      }));
      let bulkRelTrack = [];
      tableRelMapping.forEach((table) => {
        
        // check relations.obj and relations.arr length and form queries
        if (table.relations.objectRel.length) {
          table.relations.objectRel.forEach((indivObjectRel) => {
            indivObjectRel.relName = formRelName(
              indivObjectRel,
              table.existingFields
            );
            /* Added to ensure that fallback relationship name is created in case of tracking all relationship at once */
            table.existingFields[indivObjectRel.relName] = true;
            const { upQuery, downQuery } = generateRelationshipsQuery(
              indivObjectRel,
              currentSource
            );

            const objTrack = {
              upQuery,
              downQuery,
              data: indivObjectRel,
            };
           
            bulkRelTrack.push(objTrack);
            // console.log(
            //   `${currentSchema} ${objTrack.data.relName} [object] [not tracked]`
            // );

            // [4] track object relationships
            if (false) {
              let res = updateMetaData(objTrack.upQuery);
              res.then((data) => {
                console.log(res);
              });
            }
          });
        }

        if (table.relations.arrayRel.length) {
          table.relations.arrayRel.forEach((indivArrayRel) => {
            indivArrayRel.relName = formRelName(
              indivArrayRel,
              table.existingFields
            );
            /* Added to ensure that fallback relationship name is created in case of tracking all relationship at once */
            table.existingFields[indivArrayRel.relName] = true;
            const { upQuery, downQuery } = generateRelationshipsQuery(
              indivArrayRel,
              currentSource
            );

            const arrTrack = {
              upQuery,
              downQuery,
              data: indivArrayRel,
            };
            
            bulkRelTrack.push(arrTrack);
            // console.log(
            //   `${currentSchema} ${arrTrack.data.relName} [array] [not tracked]`
            // );

            // [5] track array relationships
            if (false) {
              let res = updateMetaData(arrTrack.upQuery);
              res.then((data) => {
                console.log(res);
              });
            }
          });
        }
      });
      // [6] - all suggested relationships grouped by schema
      untrackedRelations[currentSchema] = bulkRelTrack;
    });
  }
}

main();
