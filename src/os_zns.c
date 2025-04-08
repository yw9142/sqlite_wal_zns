/*
** 2023 ZNS SSD Optimized VFS for SQLite
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains the VFS implementation for ZNS SSD optimized WAL mode.
** It is designed to work with the existing WAL implementation in wal.c
** but provides optimal write patterns for ZNS SSD devices.
**
** This VFS is based on the standard Unix VFS but adds special handling
** for WAL files when they are located on ZNS SSD devices.
*/

#include "sqlite3.h"
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <linux/fs.h>
#include <linux/blkzoned.h>
#include <dirent.h>

/* ZNS 관련 상수 정의 - 시스템에 없을 경우 대체 정의 */
#ifndef BLKRESETZONE
#define BLKRESETZONE _IOW(0x12, 103, struct blk_zone_range)
#endif

/* ZNS Zone 관리를 위한 구조체 */
typedef struct zns_zone_manager zns_zone_manager;
struct zns_zone_manager
{
    char *zZnsPath;       /* ZNS SSD 경로 (zonefs 마운트 포인트) */
    int nZones;           /* 사용 가능한 Zone의 수 */
    int *aZoneState;      /* Zone 사용 상태 (0: 미사용, 1: 사용 중) */
    char **aZoneFiles;    /* Zone 파일 경로 배열 */
    sqlite3_mutex *mutex; /* Zone 관리 뮤텍스 */
};

/* 전역 Zone 관리자 */
static zns_zone_manager zoneManager = {0};

/* zone file name에서 zonefs에서 사용하는 파일 형식 */
#define ZONEFS_SEQ_FILE_PATTERN "%04x"

/* Forward declarations */
static sqlite3_vfs *pOrigVfs = 0; /* Pointer to the original VFS */
static int znsVfsInit(sqlite3_vfs *);

/* External declarations from wal.c */
extern int sqlite3WalUseZnsSsd(void);
extern const char *sqlite3WalGetZnsSsdPath(void);

/*
** Custom file structure that wraps the standard Unix file
** with additional ZNS SSD specific information
*/
typedef struct zns_file zns_file;
struct zns_file
{
    sqlite3_file base;   /* Base class. Must be first */
    sqlite3_file *pReal; /* The real underlying file */
    char *zPath;         /* Copy of filename */
    int isWal;           /* True if this is a WAL file */
    int isZnsWal;        /* True if this WAL file is on ZNS SSD */
};

/*
** Method declarations for zns_file
*/
static int znsClose(sqlite3_file *);
static int znsRead(sqlite3_file *, void *, int iAmt, sqlite3_int64 iOfst);
static int znsWrite(sqlite3_file *, const void *, int iAmt, sqlite3_int64 iOfst);
static int znsTruncate(sqlite3_file *, sqlite3_int64 size);
static int znsSync(sqlite3_file *, int flags);
static int znsFileSize(sqlite3_file *, sqlite3_int64 *pSize);
static int znsLock(sqlite3_file *, int);
static int znsUnlock(sqlite3_file *, int);
static int znsCheckReservedLock(sqlite3_file *, int *pResOut);
static int znsFileControl(sqlite3_file *, int op, void *pArg);
static int znsSectorSize(sqlite3_file *);
static int znsDeviceCharacteristics(sqlite3_file *);
static int znsShmMap(sqlite3_file *, int iPg, int pgsz, int, void volatile **);
static int znsShmLock(sqlite3_file *, int offset, int n, int flags);
static void znsShmBarrier(sqlite3_file *);
static int znsShmUnmap(sqlite3_file *, int deleteFlag);
static int znsFetch(sqlite3_file *, sqlite3_int64 iOfst, int iAmt, void **pp);
static int znsUnfetch(sqlite3_file *, sqlite3_int64 iOfst, void *p);

/*
** The zns_file_methods object specifies the methods for
** the ZNS SSD VFS implementation
*/
static const sqlite3_io_methods zns_file_methods = {
    2,                        /* iVersion */
    znsClose,                 /* xClose */
    znsRead,                  /* xRead */
    znsWrite,                 /* xWrite */
    znsTruncate,              /* xTruncate */
    znsSync,                  /* xSync */
    znsFileSize,              /* xFileSize */
    znsLock,                  /* xLock */
    znsUnlock,                /* xUnlock */
    znsCheckReservedLock,     /* xCheckReservedLock */
    znsFileControl,           /* xFileControl */
    znsSectorSize,            /* xSectorSize */
    znsDeviceCharacteristics, /* xDeviceCharacteristics */
    znsShmMap,                /* xShmMap */
    znsShmLock,               /* xShmLock */
    znsShmBarrier,            /* xShmBarrier */
    znsShmUnmap,              /* xShmUnmap */
    znsFetch,                 /* xFetch */
    znsUnfetch                /* xUnfetch */
};

/* ZNS VFS structure that extends the standard VFS */
typedef struct zns_vfs zns_vfs;
struct zns_vfs
{
    sqlite3_vfs base;      /* Base class - must be first */
    sqlite3_vfs *pRealVfs; /* The real underlying VFS */
};

/* Helper function to check if a path is a WAL file on a ZNS device */
static int isZnsWalFile(const char *zPath)
{
    /* Check if ZNS mode is enabled */
    if (!sqlite3WalUseZnsSsd())
        return 0;

    /* Check if this is a WAL file (ends with -wal) */
    int nPath = strlen(zPath);
    if (nPath <= 4)
        return 0;
    if (sqlite3_strnicmp(&zPath[nPath - 4], "-wal", 4) != 0)
        return 0;

    /* Check if the WAL file might be on the ZNS SSD path */
    const char *zZnsPath = sqlite3WalGetZnsSsdPath();
    if (!zZnsPath)
        return 0;

    /* If the path starts with the ZNS path, it's a ZNS WAL file */
    if (strncmp(zPath, zZnsPath, strlen(zZnsPath)) == 0)
        return 1;

    return 0;
}

/* Helper to convert regular WAL path to ZNS WAL path */
static char *getZnsWalPath(const char *zPath)
{
    char *zResult = NULL;

    if (!sqlite3WalUseZnsSsd() || !zPath)
        return NULL;

    /* Check if this is a WAL file */
    int nPath = strlen(zPath);
    if (nPath <= 4)
        return NULL;
    if (sqlite3_strnicmp(&zPath[nPath - 4], "-wal", 4) != 0)
        return NULL;

    /* Get the base filename without path */
    const char *zBase = strrchr(zPath, '/');
    if (!zBase)
    {
        zBase = zPath;
    }
    else
    {
        zBase++; /* Skip the / */
    }

    /* Combine ZNS path with base filename */
    const char *zZnsPath = sqlite3WalGetZnsSsdPath();
    if (!zZnsPath)
        return NULL;

    zResult = sqlite3_mprintf("%s/%s", zZnsPath, zBase);
    return zResult;
}

/*
** Reset a ZNS zone (or perform equivalent operation for the WAL file)
** This is implementation-specific and would be replaced with actual
** ZNS zone reset operations in a real implementation
*/
static int resetZnsZone(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;
    int fd, rc = SQLITE_OK;

    if (!p->isZnsWal)
        return SQLITE_OK;

    /* Get the file descriptor */
    rc = p->pReal->pMethods->xFileControl(p->pReal, SQLITE_FCNTL_FILE_DESCRIPTOR, &fd);
    if (rc != SQLITE_OK)
        return rc;

    /* For ZNS SSD, we would typically use a zone reset operation.
    ** This is hardware-specific, but might use an ioctl like:
    ** ioctl(fd, ZNS_RESET_ZONE, &range);
    **
    ** For this example, we'll use BLKZEROOUT which is available on many Linux systems
    ** and has a similar effect (clearing a range of blocks)
    */
#ifdef BLKZEROOUT
    unsigned long long range[2] = {0, 0}; /* start offset, length */
    struct stat st;

    /* Get the file size */
    if (fstat(fd, &st) != 0)
    {
        return SQLITE_IOERR;
    }

    /* Set the range to zero out the entire file */
    range[1] = st.st_size;

    /* Issue the ioctl to zero out the range */
    if (ioctl(fd, BLKZEROOUT, &range) != 0)
    {
        return SQLITE_IOERR;
    }
#endif

    return SQLITE_OK;
}

/*
** Close a zns-file.
*/
static int znsClose(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;
    int rc;

    /* Release the underlying file */
    rc = p->pReal->pMethods->xClose(p->pReal);

    /* Free the filename copy */
    sqlite3_free(p->zPath);
    p->zPath = NULL;

    return rc;
}

/*
** Read data from a zns-file.
*/
static int znsRead(
    sqlite3_file *pFile,
    void *zBuf,
    int iAmt,
    sqlite3_int64 iOfst)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
}

/*
** Write data to a zns-file.
*/
static int znsWrite(
    sqlite3_file *pFile,
    const void *zBuf,
    int iAmt,
    sqlite3_int64 iOfst)
{
    zns_file *p = (zns_file *)pFile;
    int rc;

    /* ZNS SSD WAL 파일의 경우 특별 처리 */
    if (p->isZnsWal)
    {
        int fd;
        i64 iSize;

        /* 파일 디스크립터 얻기 */
        rc = p->pReal->pMethods->xFileControl(p->pReal, SQLITE_FCNTL_FILE_DESCRIPTOR, &fd);
        if (rc != SQLITE_OK)
            return rc;

        /* 현재 파일 크기 확인 */
        rc = p->pReal->pMethods->xFileSize(p->pReal, &iSize);
        if (rc != SQLITE_OK)
            return rc;

        /* ZNS에서는 항상 순차적 쓰기가 필요함 */
        if (iOfst != iSize)
        {
            /* WAL 파일의 헤더 업데이트 등을 처리하기 위한 특별 케이스 */
            if (iOfst < 32)
            { /* WAL 헤더 크기는 32바이트 */
                /* WAL 헤더 쓰기는 허용 */
                return p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
            }

            /* 그 외에는 순차 쓰기만 허용 - iOfst가 현재 크기와 다르면 에러 */
            return SQLITE_IOERR_WRITE;
        }

        /* 순차 쓰기 - zonefs에서는 항상 append 모드로 쓰기 */
        rc = p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);

        /* Zone이 가득 찼을 때 처리 */
        if (rc == SQLITE_FULL || rc == SQLITE_IOERR_WRITE)
        {
            /* 여기에서 새로운 Zone을 할당하는 로직을 구현할 수 있음 */
            /* 이 구현에서는 상위 레이어(WAL)가 에러를 받아 체크포인트를 수행하도록 함 */
        }

        return rc;
    }

    /* ZNS가 아닌 파일은 원래 VFS로 처리 */
    return p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
}

/*
** Truncate a zns-file.
*/
static int znsTruncate(sqlite3_file *pFile, sqlite3_int64 size)
{
    zns_file *p = (zns_file *)pFile;

    /* For ZNS WAL files, truncate might be implemented with zone reset */
    if (p->isZnsWal && size == 0)
    {
        int rc = resetZnsZone(pFile);
        if (rc != SQLITE_OK)
            return rc;
    }

    return p->pReal->pMethods->xTruncate(p->pReal, size);
}

/*
** Sync a zns-file.
*/
static int znsSync(sqlite3_file *pFile, int flags)
{
    zns_file *p = (zns_file *)pFile;

    /* For ZNS WAL files, fsync might be unnecessary due to
    ** ZNS write semantics, but we still call the underlying fsync
    ** for safety unless we're certain about the hardware behavior
    */
    return p->pReal->pMethods->xSync(p->pReal, flags);
}

/*
** Return the current file-size of a zns-file.
*/
static int znsFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}

/*
** Lock a zns-file.
*/
static int znsLock(sqlite3_file *pFile, int eLock)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xLock(p->pReal, eLock);
}

/*
** Unlock a zns-file.
*/
static int znsUnlock(sqlite3_file *pFile, int eLock)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}

/*
** Check if another file-handle holds a RESERVED lock on a zns-file.
*/
static int znsCheckReservedLock(sqlite3_file *pFile, int *pResOut)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
}

/*
** File control method. For custom operations on a zns-file.
*/
static int znsFileControl(sqlite3_file *pFile, int op, void *pArg)
{
    zns_file *p = (zns_file *)pFile;
    int rc;

    /* Handle ZNS-specific operations */
    switch (op)
    {
    /* Add custom ZNS-specific file control operations here */

    /* Handle WAL-specific operations */
    case SQLITE_FCNTL_WAL_CHECKPOINT:
        /* Special handling for WAL checkpoints on ZNS SSD */
        if (p->isZnsWal)
        {
            /* Prepare for checkpoint (eg: zone management operations) */
        }
        break;

    case SQLITE_FCNTL_JOURNAL_POINTER:
        /* Handle journal pointer operations for ZNS */
        break;
    }

    /* Pass through to the real VFS */
    rc = p->pReal->pMethods->xFileControl(p->pReal, op, pArg);

    return rc;
}

/*
** Return the sector-size in bytes for a zns-file.
*/
static int znsSectorSize(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;

    /* For ZNS SSDs, we might want to return the zone size or sector size */
    if (p->isZnsWal)
    {
        /* In a real implementation, we would query the actual ZNS zone size */
        /* For now, just return a larger value typical for ZNS zones */
        return 4096; /* or larger, depending on actual ZNS hardware */
    }

    return p->pReal->pMethods->xSectorSize(p->pReal);
}

/*
** Return the device characteristics for a zns-file.
*/
static int znsDeviceCharacteristics(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;

    /* For ZNS WAL files, add special device characteristics */
    if (p->isZnsWal)
    {
        /* ZNS writes are append-only within a zone and zones can be reset */
        return p->pReal->pMethods->xDeviceCharacteristics(p->pReal) |
               SQLITE_IOCAP_SEQUENTIAL |
               SQLITE_IOCAP_SAFE_APPEND;
    }

    return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
}

/*
** The shared memory related methods - mostly pass-through to the real VFS
*/
static int znsShmMap(
    sqlite3_file *pFile,
    int iPg,
    int pgsz,
    int isWrite,
    void volatile **pp)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xShmMap(p->pReal, iPg, pgsz, isWrite, pp);
}

static int znsShmLock(sqlite3_file *pFile, int offset, int n, int flags)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xShmLock(p->pReal, offset, n, flags);
}

static void znsShmBarrier(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;
    p->pReal->pMethods->xShmBarrier(p->pReal);
}

static int znsShmUnmap(sqlite3_file *pFile, int deleteFlag)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xShmUnmap(p->pReal, deleteFlag);
}

/*
** Fetch and unfetch methods - added in SQLite version 3.10.0
*/
static int znsFetch(
    sqlite3_file *pFile,
    sqlite3_int64 iOfst,
    int iAmt,
    void **pp)
{
    zns_file *p = (zns_file *)pFile;
    /* Pass through to real implementation if it exists */
    if (p->pReal->pMethods->xFetch)
    {
        return p->pReal->pMethods->xFetch(p->pReal, iOfst, iAmt, pp);
    }
    else
    {
        *pp = 0;
        return SQLITE_OK;
    }
}

static int znsUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pPage)
{
    zns_file *p = (zns_file *)pFile;
    /* Pass through to real implementation if it exists */
    if (p->pReal->pMethods->xUnfetch)
    {
        return p->pReal->pMethods->xUnfetch(p->pReal, iOfst, pPage);
    }
    else
    {
        return SQLITE_OK;
    }
}

/*
** Open a file handle.
*/
static int znsOpen(
    sqlite3_vfs *pVfs,
    const char *zName,
    sqlite3_file *pFile,
    int flags,
    int *pOutFlags)
{
    zns_file *p = (zns_file *)pFile;
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    int rc = SQLITE_OK;
    char *zZnsPath = NULL;
    char *zZoneFile = NULL;
    int isWal = 0;
    int isZnsWal = 0;

    /* Check if this is a WAL file */
    if (zName && (flags & SQLITE_OPEN_WAL))
    {
        isWal = 1;

        /* 만약 ZNS 모드가 활성화된 경우 */
        if (sqlite3WalUseZnsSsd())
        {
            /* 기존 구현: zName 경로를 ZNS 경로로 변환 */
            /* 그러나 zonefs에서는 임의 파일명 생성이 불가능하므로
             * 사용 가능한 존 파일을 찾아야 함 */
            zZoneFile = znsGetFreeZoneFile(zName);

            if (zZoneFile)
            {
                /* 존 파일을 찾았다면 이 경로를 대신 사용 */
                zName = zZoneFile;
                isZnsWal = 1;
            }
            else
            {
                /* 사용 가능한 존 파일이 없으면 원래 경로 사용 */
                return SQLITE_FULL;
            }
        }
    }

    /* Initialize the zns_file structure */
    memset(p, 0, sizeof(zns_file));
    p->pReal = (sqlite3_file *)&p[1]; /* Real file structure is stored right after */

    /* Open the underlying file using the original VFS */
    /* zonefs에서는 파일을 생성할 수 없고, O_CREAT 플래그를 제거해야 함 */
    int modifiedFlags = flags;
    if (isZnsWal)
    {
        modifiedFlags &= ~(SQLITE_OPEN_CREATE | SQLITE_OPEN_DELETEONCLOSE);
    }

    rc = pZnsVfs->pRealVfs->xOpen(pZnsVfs->pRealVfs, zName, p->pReal, modifiedFlags, pOutFlags);
    if (rc != SQLITE_OK)
    {
        if (isZnsWal && zZoneFile)
        {
            /* 열기 실패 시 상태를 '미사용'으로 되돌림 */
            sqlite3_mutex_enter(zoneManager.mutex);
            for (int i = 0; i < zoneManager.nZones; i++)
            {
                if (zoneManager.aZoneFiles[i] == zZoneFile)
                {
                    zoneManager.aZoneState[i] = 0; /* 미사용 상태로 변경 */
                    break;
                }
            }
            sqlite3_mutex_leave(zoneManager.mutex);
        }
        return rc;
    }

    /* Save the file information */
    p->isWal = isWal;
    p->isZnsWal = isZnsWal;
    p->zPath = zZoneFile ? sqlite3_mprintf("%s", zZoneFile) : (zName ? sqlite3_mprintf("%s", zName) : NULL);

    /* Set up the zns-file methods */
    p->base.pMethods = &zns_file_methods;

    return SQLITE_OK;
}

/*
** Delete the file located at zPath. If the dirSync parameter is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int znsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    int isWal = 0;
    char *zZnsPath = NULL;
    int rc = SQLITE_OK;

    /* Check if this is a WAL file that might be on ZNS SSD */
    if (zPath)
    {
        int nPath = strlen(zPath);
        if (nPath > 4 && sqlite3_strnicmp(&zPath[nPath - 4], "-wal", 4) == 0)
        {
            isWal = 1;

            /* If this is a WAL file and ZNS mode is enabled,
            ** we might need to delete the file from ZNS path too */
            if (sqlite3WalUseZnsSsd())
            {
                zZnsPath = getZnsWalPath(zPath);

                /* First try to delete from the ZNS location */
                if (zZnsPath)
                {
                    rc = pZnsVfs->pRealVfs->xDelete(pZnsVfs->pRealVfs, zZnsPath, dirSync);
                    sqlite3_free(zZnsPath);

                    /* If we successfully deleted the ZNS WAL file, we're done */
                    if (rc == SQLITE_OK)
                        return rc;
                }
            }
        }
    }

    /* Delete the file using the original VFS */
    return pZnsVfs->pRealVfs->xDelete(pZnsVfs->pRealVfs, zPath, dirSync);
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int znsAccess(
    sqlite3_vfs *pVfs,
    const char *zPath,
    int flags,
    int *pResOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    int isWal = 0;
    char *zZnsPath = NULL;

    /* Check if this is a WAL file */
    if (zPath)
    {
        int nPath = strlen(zPath);
        if (nPath > 4 && sqlite3_strnicmp(&zPath[nPath - 4], "-wal", 4) == 0)
        {
            isWal = 1;

            /* If this is a WAL file and ZNS mode is enabled, check the ZNS path */
            if (sqlite3WalUseZnsSsd())
            {
                zZnsPath = getZnsWalPath(zPath);

                /* Check access permissions for the ZNS path */
                if (zZnsPath)
                {
                    int rc = pZnsVfs->pRealVfs->xAccess(pZnsVfs->pRealVfs, zZnsPath, flags, pResOut);
                    sqlite3_free(zZnsPath);

                    /* If the file exists in the ZNS path, we're done */
                    if (rc == SQLITE_OK && *pResOut)
                        return rc;
                }
            }
        }
    }

    /* Fall back to the original VFS */
    return pZnsVfs->pRealVfs->xAccess(pZnsVfs->pRealVfs, zPath, flags, pResOut);
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (SQLITE_MAX_PATHNAME+1) bytes.
*/
static int znsFullPathname(
    sqlite3_vfs *pVfs,
    const char *zPath,
    int nOut,
    char *zOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    return pZnsVfs->pRealVfs->xFullPathname(pZnsVfs->pRealVfs, zPath, nOut, zOut);
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *znsDlOpen(sqlite3_vfs *pVfs, const char *zPath)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    return pZnsVfs->pRealVfs->xDlOpen(pZnsVfs->pRealVfs, zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated
** with dynamic libraries.
*/
static void znsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    pZnsVfs->pRealVfs->xDlError(pZnsVfs->pRealVfs, nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*znsDlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    return pZnsVfs->pRealVfs->xDlSym(pZnsVfs->pRealVfs, p, zSym);
}

/*
** Close the dynamic library handle pHandle.
*/
static void znsDlClose(sqlite3_vfs *pVfs, void *pHandle)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    pZnsVfs->pRealVfs->xDlClose(pZnsVfs->pRealVfs, pHandle);
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of
** random data.
*/
static int znsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    return pZnsVfs->pRealVfs->xRandomness(pZnsVfs->pRealVfs, nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds
** actually slept.
*/
static int znsSleep(sqlite3_vfs *pVfs, int nMicro)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    return pZnsVfs->pRealVfs->xSleep(pZnsVfs->pRealVfs, nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int znsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    return pZnsVfs->pRealVfs->xCurrentTime(pZnsVfs->pRealVfs, pTimeOut);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
** This version includes the fractional microseconds.
*/
static int znsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    if (pZnsVfs->pRealVfs->xCurrentTimeInt64)
    {
        return pZnsVfs->pRealVfs->xCurrentTimeInt64(pZnsVfs->pRealVfs, pTimeOut);
    }
    else
    {
        double d;
        int rc = pZnsVfs->pRealVfs->xCurrentTime(pZnsVfs->pRealVfs, &d);
        if (rc == SQLITE_OK)
        {
            *pTimeOut = (sqlite3_int64)(d * 86400000.0);
        }
        return rc;
    }
}

/*
** Return the last error code and message for this VFS
*/
static int znsGetLastError(sqlite3_vfs *pVfs, int nBuf, char *zBuf)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs;
    if (pZnsVfs->pRealVfs->xGetLastError)
    {
        return pZnsVfs->pRealVfs->xGetLastError(pZnsVfs->pRealVfs, nBuf, zBuf);
    }
    return 0;
}

/*
** Zone 관리자 초기화 함수
*/
static int znsZoneManagerInit(const char *zZnsPath)
{
    DIR *dir;
    struct dirent *entry;
    int nZones = 0;
    char **aFiles = NULL;
    int i = 0;

    if (!zZnsPath || !zZnsPath[0])
        return SQLITE_ERROR;

    /* 이미 초기화된 경우 */
    if (zoneManager.zZnsPath)
        return SQLITE_OK;

    /* ZNS 경로 열기 */
    dir = opendir(zZnsPath);
    if (!dir)
        return SQLITE_ERROR;

    /* 존 파일 개수 세기 (zonefs의 형식은 일반적으로 0000, 0001, ... 의 형식) */
    while ((entry = readdir(dir)) != NULL)
    {
        if (entry->d_type == DT_REG)
        {
            unsigned int zoneNum;
            /* 16진수 형식으로 된 파일 이름인지 확인 (예: "0000") */
            if (sscanf(entry->d_name, ZONEFS_SEQ_FILE_PATTERN, &zoneNum) == 1)
            {
                nZones++;
            }
        }
    }
    rewinddir(dir);

    /* 메모리 할당 */
    zoneManager.zZnsPath = sqlite3_mprintf("%s", zZnsPath);
    zoneManager.nZones = nZones;
    zoneManager.aZoneState = sqlite3_malloc(sizeof(int) * nZones);
    zoneManager.aZoneFiles = sqlite3_malloc(sizeof(char *) * nZones);
    zoneManager.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);

    if (!zoneManager.zZnsPath || !zoneManager.aZoneState ||
        !zoneManager.aZoneFiles || !zoneManager.mutex)
    {
        /* 메모리 할당 실패 시 정리 */
        sqlite3_free(zoneManager.zZnsPath);
        sqlite3_free(zoneManager.aZoneState);
        sqlite3_free(zoneManager.aZoneFiles);
        if (zoneManager.mutex)
            sqlite3_mutex_free(zoneManager.mutex);

        memset(&zoneManager, 0, sizeof(zoneManager));
        closedir(dir);
        return SQLITE_NOMEM;
    }

    /* 초기값 설정 */
    memset(zoneManager.aZoneState, 0, sizeof(int) * nZones);
    memset(zoneManager.aZoneFiles, 0, sizeof(char *) * nZones);

    /* 존 파일들의 경로를 저장 */
    i = 0;
    while ((entry = readdir(dir)) != NULL && i < nZones)
    {
        if (entry->d_type == DT_REG)
        {
            unsigned int zoneNum;
            if (sscanf(entry->d_name, ZONEFS_SEQ_FILE_PATTERN, &zoneNum) == 1)
            {
                zoneManager.aZoneFiles[i] = sqlite3_mprintf(
                    "%s/%s", zZnsPath, entry->d_name);
                i++;
            }
        }
    }
    closedir(dir);

    return SQLITE_OK;
}

/*
** Zone 관리자 해제 함수
*/
static void znsZoneManagerDestroy(void)
{
    int i;

    if (!zoneManager.zZnsPath)
        return;

    sqlite3_mutex_enter(zoneManager.mutex);

    sqlite3_free(zoneManager.zZnsPath);
    sqlite3_free(zoneManager.aZoneState);

    for (i = 0; i < zoneManager.nZones; i++)
    {
        sqlite3_free(zoneManager.aZoneFiles[i]);
    }
    sqlite3_free(zoneManager.aZoneFiles);

    sqlite3_mutex_leave(zoneManager.mutex);
    sqlite3_mutex_free(zoneManager.mutex);

    memset(&zoneManager, 0, sizeof(zoneManager));
}

/*
** 사용 가능한 zone 파일 찾기
*/
static char *znsGetFreeZoneFile(const char *zWalName)
{
    int i;
    char *zZoneFile = NULL;

    if (!zoneManager.zZnsPath || !zoneManager.aZoneFiles)
    {
        znsZoneManagerInit(sqlite3WalGetZnsSsdPath());
    }

    if (!zoneManager.zZnsPath || !zoneManager.aZoneFiles)
    {
        return NULL;
    }

    sqlite3_mutex_enter(zoneManager.mutex);

    /* 이미 이 WAL 파일용으로 할당된 Zone이 있는지 확인 */
    for (i = 0; i < zoneManager.nZones; i++)
    {
        if (zoneManager.aZoneState[i] == 2)
        {
            /* WAL 파일 이름이 마지막 경로 성분만 비교 */
            const char *zBaseName = strrchr(zWalName, '/');
            zBaseName = zBaseName ? zBaseName + 1 : zWalName;

            const char *zZoneBaseName = strrchr(zoneManager.aZoneFiles[i], '/');
            zZoneBaseName = zZoneBaseName ? zZoneBaseName + 1 : zoneManager.aZoneFiles[i];

            /* 이미 사용 중인 zone 파일 반환 */
            if (strcmp(zBaseName, zZoneBaseName) == 0)
            {
                zZoneFile = zoneManager.aZoneFiles[i];
                break;
            }
        }
    }

    /* 사용 가능한 Zone 찾기 */
    if (!zZoneFile)
    {
        for (i = 0; i < zoneManager.nZones; i++)
        {
            if (zoneManager.aZoneState[i] == 0)
            {
                zoneManager.aZoneState[i] = 1; /* 사용 중 표시 */
                zZoneFile = zoneManager.aZoneFiles[i];
                break;
            }
        }
    }

    sqlite3_mutex_leave(zoneManager.mutex);
    return zZoneFile;
}

/*
** Initialize the ZNS VFS module.
** This routine registers the ZNS VFS with SQLite.
*/
static int znsVfsInit(sqlite3_vfs *pOrigVfs)
{
    static sqlite3_vfs zns_vfs;
    static zns_vfs zns_vfs_impl;

    /* Store the original VFS pointer */
    if (pOrigVfs == 0)
    {
        /* If no VFS specified, use the default unix VFS */
        pOrigVfs = sqlite3_vfs_find("unix");
        if (pOrigVfs == 0)
            return SQLITE_ERROR;
    }

    /* Initialize the wrapper VFS */
    memset(&zns_vfs_impl, 0, sizeof(zns_vfs_impl));
    zns_vfs_impl.pRealVfs = pOrigVfs;

    /* Initialize the public VFS structure */
    memset(&zns_vfs, 0, sizeof(sqlite3_vfs));
    zns_vfs.iVersion = 3;                                     /* Structure version number */
    zns_vfs.szOsFile = sizeof(zns_file) + pOrigVfs->szOsFile; /* Size of subclassed sqlite3_file */
    zns_vfs.mxPathname = pOrigVfs->mxPathname;                /* Maximum path length */
    zns_vfs.pNext = 0;                                        /* Next registered VFS */
    zns_vfs.zName = "zns";                                    /* Name of this VFS */
    zns_vfs.pAppData = &zns_vfs_impl;                         /* Pointer to ZNS VFS */

    /* IO methods */
    zns_vfs.xOpen = znsOpen;
    zns_vfs.xDelete = znsDelete;
    zns_vfs.xAccess = znsAccess;
    zns_vfs.xFullPathname = znsFullPathname;
    zns_vfs.xDlOpen = znsDlOpen;
    zns_vfs.xDlError = znsDlError;
    zns_vfs.xDlSym = znsDlSym;
    zns_vfs.xDlClose = znsDlClose;
    zns_vfs.xRandomness = znsRandomness;
    zns_vfs.xSleep = znsSleep;
    zns_vfs.xCurrentTime = znsCurrentTime;
    zns_vfs.xGetLastError = znsGetLastError;
    zns_vfs.xCurrentTimeInt64 = znsCurrentTimeInt64;

    /* Register the ZNS VFS */
    return sqlite3_vfs_register(&zns_vfs, 0);
}

/*
** External API function to initialize the ZNS VFS extension
*/
int sqlite3_zns_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi)
{
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi);

    /* Save the original VFS */
    if (pOrigVfs == 0)
    {
        pOrigVfs = sqlite3_vfs_find("unix");
        if (pOrigVfs == 0)
            return SQLITE_ERROR;
    }

    /* Initialize the ZNS VFS */
    rc = znsVfsInit(pOrigVfs);

    /* Set this VFS as the default if requested */
    /* sqlite3_vfs_register(sqlite3_vfs_find("zns"), 1); */

    return rc;
}

/* Export public API functions to set ZNS path and enable ZNS mode */
int sqlite3_wal_use_zns(const char *znsPath)
{
    extern void sqlite3WalSetZnsSsdPath(const char *zPath);
    extern void sqlite3WalEnableZnsSsd(int enable);

    if (znsPath == 0 || znsPath[0] == 0)
    {
        sqlite3WalEnableZnsSsd(0);
        sqlite3WalSetZnsSsdPath(0);
        return SQLITE_OK;
    }

    /* Check if the path is valid */
    struct stat st;
    if (stat(znsPath, &st) != 0 || !S_ISDIR(st.st_mode))
    {
        return SQLITE_ERROR;
    }

    /* Set the path and enable ZNS mode */
    sqlite3WalSetZnsSsdPath(znsPath);
    sqlite3WalEnableZnsSsd(1);

    return SQLITE_OK;
}