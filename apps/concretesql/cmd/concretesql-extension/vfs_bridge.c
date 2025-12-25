#include <string.h>
#include <stdio.h>
#include <time.h>
#include "sqlite3ext.h"

SQLITE_EXTENSION_INIT3

/* 
   Access Go functions exported via CGO.
   We declare them here matching the Go `//export` signatures.
*/
extern int go_vfs_open(char *name, void **fileOut, int flags, int *outFlags);
extern int go_vfs_delete(char *name, int syncDir);
extern int go_vfs_access(char *name, int flags, int *pResOut);
extern int go_vfs_full_pathname(char *name, int nOut, char *zOut);

// File Methods
extern int go_file_close(void *p);
extern int go_file_read(void *p, void *buf, int iAmt, long long iOfst);
extern int go_file_write(void *p, void *buf, int iAmt, long long iOfst);
extern int go_file_truncate(void *p, long long size);
extern int go_file_sync(void *p, int flags);
extern int go_file_file_size(void *p, long long *pSize);
extern int go_file_lock(void *p, int lock);
extern int go_file_unlock(void *p, int lock);
extern int go_file_check_reserved_lock(void *p, int *pResOut);
extern int go_file_sector_size(void *p);
extern int go_file_device_characteristics(void *p);

typedef struct ConcreteSQLFile {
    sqlite3_file base;
    void *goFileHandle; // Pointer to Go object (via cgo.Handle or map index)
} ConcreteSQLFile;

// --- File Method Wrappers ---

int xClose(sqlite3_file *pFile) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    int rc = go_file_close(p->goFileHandle);
    return rc;
}

int xRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite_int64 iOfst) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_read(p->goFileHandle, zBuf, iAmt, iOfst);
}

int xWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite_int64 iOfst) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    // Cast const away, Go wrapper will copy
    return go_file_write(p->goFileHandle, (void*)zBuf, iAmt, iOfst);
}

int xTruncate(sqlite3_file *pFile, sqlite_int64 size) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_truncate(p->goFileHandle, size);
}

int xSync(sqlite3_file *pFile, int flags) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_sync(p->goFileHandle, flags);
}

int xFileSize(sqlite3_file *pFile, sqlite_int64 *pSize) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    long long sz = 0;
    int rc = go_file_file_size(p->goFileHandle, &sz);
    *pSize = sz;
    return rc;
}

int xLock(sqlite3_file *pFile, int lock) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_lock(p->goFileHandle, lock);
}

int xUnlock(sqlite3_file *pFile, int lock) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_unlock(p->goFileHandle, lock);
}

int xCheckReservedLock(sqlite3_file *pFile, int *pResOut) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_check_reserved_lock(p->goFileHandle, pResOut);
}

int xFileControl(sqlite3_file *pFile, int op, void *pArg) {
    return SQLITE_NOTFOUND;
}

int xSectorSize(sqlite3_file *pFile) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_sector_size(p->goFileHandle);
}

int xDeviceCharacteristics(sqlite3_file *pFile) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    return go_file_device_characteristics(p->goFileHandle);
}

static const sqlite3_io_methods concretesql_io_methods = {
    1,                            /* iVersion */
    xClose,                       /* xClose */
    xRead,                        /* xRead */
    xWrite,                       /* xWrite */
    xTruncate,                    /* xTruncate */
    xSync,                        /* xSync */
    xFileSize,                    /* xFileSize */
    xLock,                        /* xLock */
    xUnlock,                      /* xUnlock */
    xCheckReservedLock,           /* xCheckReservedLock */
    xFileControl,                 /* xFileControl */
    xSectorSize,                  /* xSectorSize */
    xDeviceCharacteristics,       /* xDeviceCharacteristics */
    0,                            /* xShmMap */
    0,                            /* xShmLock */
    0,                            /* xShmBarrier */
    0,                            /* xShmUnmap */
    0,                            /* xFetch */
    0                             /* xUnfetch */
};

// --- VFS Method Wrappers ---

int xOpen(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags) {
    ConcreteSQLFile *p = (ConcreteSQLFile*)pFile;
    p->base.pMethods = &concretesql_io_methods;
    p->goFileHandle = 0;

    int outFlags = 0;
    void *goHandle = 0;
    
    // Call Go
    int rc = go_vfs_open((char*)zName, &goHandle, flags, &outFlags);
    
    if (rc == SQLITE_OK) {
        p->goFileHandle = goHandle;
        if (pOutFlags) *pOutFlags = outFlags;
    }
    return rc;
}

int xDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir) {
    return go_vfs_delete((char*)zName, syncDir);
}

int xAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut) {
    return go_vfs_access((char*)zName, flags, pResOut);
}

int xFullPathname(sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut) {
    return go_vfs_full_pathname((char*)zName, nOut, zOut);
}

void *xDlOpen(sqlite3_vfs *pVfs, const char *zFilename) { return 0; }
void xDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg) { 
    sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions not supported");
}
void (*xDlSym(sqlite3_vfs *pVfs, void *p, const char *zSymbol))(void) { return 0; }
void xDlClose(sqlite3_vfs *pVfs, void *p) { }

int xRandomness(sqlite3_vfs *pVfs, int nByte, char *zOut) { return SQLITE_OK; }
int xSleep(sqlite3_vfs *pVfs, int microseconds) { return SQLITE_OK; }
int xCurrentTime(sqlite3_vfs *pVfs, double *pTime) { return SQLITE_OK; }
int xGetLastError(sqlite3_vfs *pVfs, int n, char *z) { return 0; }

static sqlite3_vfs concretesql_vfs = {
    1,                            /* iVersion */
    0,                            /* szOsFile - set at init */
    1024,                         /* mxPathname */
    0,                            /* pNext */
    "concretesql",                /* zName */
    0,                            /* pAppData */
    xOpen,                        /* xOpen */
    xDelete,                      /* xDelete */
    xAccess,                      /* xAccess */
    xFullPathname,                /* xFullPathname */
    xDlOpen,                      /* xDlOpen */
    xDlError,                     /* xDlError */
    xDlSym,                       /* xDlSym */
    xDlClose,                     /* xDlClose */
    xRandomness,                  /* xRandomness */
    xSleep,                       /* xSleep */
    xCurrentTime,                 /* xCurrentTime */
    xGetLastError,                /* xGetLastError */
    0,                            /* xCurrentTimeInt64 */
    0,                            /* xSetSystemCall */
    0,                            /* xGetSystemCall */
    0                             /* xNextSystemCall */
};

int register_concretesql_vfs(const char *name) {
    concretesql_vfs.szOsFile = sizeof(ConcreteSQLFile);
    concretesql_vfs.zName = name;
    // Use the API pointer provided by the extension init mechanism
    return sqlite3_vfs_register(&concretesql_vfs, 1); // 1 = make default? Or 0? Let's make default for now.
}
