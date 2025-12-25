#include "sqlite3ext.h"

// Define the global API pointer
SQLITE_EXTENSION_INIT1

// Forward declaration of the Go export
// Note: CGO generates 'sqlite3_api_routines*' (non-const) for the Go function argument
extern int InitConcreteSQLVFS(sqlite3 *db, char **pzErrMsg, sqlite3_api_routines *pApi);

// Standard SQLite Extension Entry Point
int sqlite3_concretesql_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    // Cast away const to match Go's generated signature
    return InitConcreteSQLVFS(db, pzErrMsg, (sqlite3_api_routines*)pApi);
}
