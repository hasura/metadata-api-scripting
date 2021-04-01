
const fetch = require("node-fetch"); 

let DATABASE = 'bikesDB';

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
`

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
        "type": "mssql_run_sql",
        "args": {
            "source": database,
            "sql": fetchSQLTablesSQL,
            "cascade": false,
            "read_only": false
        }
    }
    let res = await fetch('http://localhost:8080/v2/query', {
        method: 'post',
        body:    JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' },
    })
    res = await res.json();
    if (res.result) {
        return res.result.slice(1).map(row => {
            return {
                schema: row[0],
                table: row[1]
            }
        })
    }
    return [];
}

async function getListOfFKRelationships(database = DATABASE) {
    let body = {
        "type": "mssql_run_sql",
        "args": {
            "source": database,
            "sql": fetchFKRelationships,
            "cascade": false,
            "read_only": false
        }
    }
    let res = await fetch('http://localhost:8080/v2/query', {
        method: 'post',
        body:    JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' },
    })
    res = await res.json();
    if (res) {
        return JSON.parse(res.result.slice(1));
    }
    return [];
}

async function getListOfTrackedTables(database = DATABASE) {
    let body = {"type":"export_metadata","args":{}};
    let res = await fetch('http://localhost:8080/v1/metadata', {
        method: 'post',
        body:    JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' },
    })
    res = await res.json();
    if (res.sources) {
        
        let tables = [];
        let array_relationhips = [];
        let object_relationships = [];


        res.sources.filter(s => s.name === DATABASE && s.kind === 'mssql')[0].tables.map(t => {
            tables.push({
                schema: t.table.schema,
                table: t.table.name
            });
            if (t.array_relationships) {
                array_relationhips.push(t.array_relationships)
            }
            if (t.object_relationships) {
                object_relationships.push(t.object_relationships)
            }
        })
        return {trackedTables: tables, trackedArrayRelationhips: array_relationhips,trackedObjectRelationhips: object_relationships}
    }
    return res.json();
}

async function trackTable(schema, table, database = DATABASE) {
    let body = {
        "type": "mssql_track_table",
        "args": {
            "table": {
                "name": table,
                "schema": schema
            },
            "source": database
        }
    };
    let res = await fetch('http://localhost:8080/v1/metadata', {
        method: 'post',
        body:    JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' },
    })
    return res.json()
}

async function main() {
    
    let allTables = await getListOfTables();
    let {trackedTables, trackedArrayRelationhips, trackedObjectRelationhips} = await getListOfTrackedTables();

    console.log(trackedTables, trackedArrayRelationhips, trackedObjectRelationhips)
    let untrackedTables = allTables.filter(t => !trackedTables.some(x => t.table === x.table && t.schema == x.schema))
    
    // //track one table at a time
    untrackedTables.forEach(async (t) => {
        let res = await trackTable(t.schema, t.table.replace(/\W/g, '_'));        
        console.log(`schema: ${t.schema} table: ${t.table} => ${JSON.stringify(res)}`)
    })

    // wip - to do: get untracked relations and send metadata query based on array/obj relationships
    // let allFKRelations = await getListOfFKRelationships();
    // let untrackedRelationhips = allFKRelations.filter(t => !trackedTables.some(x => t.table === x.table && t.schema == x.schema))
}

main();
