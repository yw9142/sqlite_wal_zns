/*
** 2023 ZNS SSD Optimized VFS for SQLite
**
** This VFS attempts to store SQLite Write-Ahead Log (WAL) files onto
** Zoned Namespace (ZNS) SSDs using a mounted zonefs filesystem.
** It redirects WAL file operations to specific zone files and handles
** ZNS constraints like sequential writes and zone resets.
**
** Key Features:
** - Redirects WAL file creation/opening to pre-existing zone files within
**   a specified zonefs mount point.
** - Manages zone allocation using a simple zone manager.
** - Implements write buffering for ZNS WAL files to handle potentially
**   non-sequential writes from SQLite's WAL mechanism (e.g., checksum rewrites)
**   by buffering in memory and flushing sequentially on sync.
** - Translates Truncate(0) and Delete operations on ZNS WAL files to
**   zone reset ioctls (BLKRESETZONE).
** - Passes through operations for non-WAL files or when ZNS mode is disabled
**   to the underlying default VFS.
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
#include <linux/blkzoned.h> // Needed for BLKRESETZONE
#include <dirent.h>
#include <stdlib.h> // For malloc, realloc, free
#include <stdio.h>  // For fprintf, stderr
#include <assert.h> // For assert()

/* ZNS 관련 상수 정의 - 시스템에 없을 경우 대체 정의 */
#ifndef BLKRESETZONE
/* Fallback definition, might need adjustment based on actual kernel version */
#warning "BLKRESETZONE not defined in headers, using fallback definition. Verify for your kernel."
#define BLKRESETZONE _IOW(0x12, 131, struct blk_zone_range)
#endif

/* ZNS Zone 관리를 위한 구조체 */
typedef struct zns_zone_manager zns_zone_manager;
struct zns_zone_manager
{
    char *zZnsPath;       /* ZNS SSD 경로 (zonefs 마운트 포인트) */
    int nZones;           /* 사용 가능한 Zone의 수 */
    int *aZoneState;      /* Zone 사용 상태 (0: Free, 1: Allocated) */
    char **aZoneFiles;    /* Zone 파일 경로 배열 */
    char **aWalNames;     /* 각 Zone에 매핑된 WAL 파일명 (base name, NULL: 매핑 없음) */
    sqlite3_mutex *mutex; /* Zone 관리 뮤텍스 */
};

/* 전역 Zone 관리자 */
static zns_zone_manager zoneManager = {0};

/* zone file name에서 zonefs에서 사용하는 파일 형식 */
#define ZONEFS_SEQ_FILE_PATTERN "%04x"

/* Forward declarations */
static sqlite3_vfs *pOrigVfs = 0; /* Pointer to the original VFS */
static int znsVfsInit(sqlite3_vfs *);
static char *znsGetFreeZoneFile(const char *zWalName, const char *currentZnsPath); // Helper declaration
static void znsZoneManagerDestroy(void);

/* External declarations (must be defined elsewhere, e.g., in wal.c or main app) */
extern int sqlite3WalUseZnsSsd(void);
extern const char *sqlite3WalGetZnsSsdPath(void);
extern void sqlite3WalSetZnsSsdPath(const char *zPath); // Assume these exist
extern void sqlite3WalEnableZnsSsd(int enable);         // Assume these exist

/*
** Custom file structure that wraps the standard Unix file
** with additional ZNS SSD specific information
*/
typedef struct zns_file zns_file;
struct zns_file
{
    sqlite3_file base;   /* Base class. Must be first */
    sqlite3_file *pReal; /* The real underlying file handle */
    char *zPath;         /* Full path to the file being used (original or zone) */
    int isWal;           /* True if this represents a WAL file */
    int isZnsWal;        /* True if this WAL file is on ZNS SSD (using a zone file) */

    /* --- ZNS WAL Buffering --- */
    unsigned char *pBuffer; /* Write buffer for ZNS WAL */
    sqlite3_int64 nBuffer;  /* Current size of data in buffer (logical size) */
    sqlite3_int64 nAlloc;   /* Allocated size of pBuffer */
    sqlite3_int64 iFlushed; /* Amount of data flushed to disk */
    // sqlite3_mutex *pBufMutex; /* Mutex for buffer access (optional, likely not needed) */
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
** Method declarations for zns_vfs
*/
static int znsOpen(sqlite3_vfs *, const char *, sqlite3_file *, int, int *);
static int znsDelete(sqlite3_vfs *, const char *, int);
static int znsAccess(sqlite3_vfs *, const char *, int, int *);
static int znsFullPathname(sqlite3_vfs *, const char *, int, char *);
static void *znsDlOpen(sqlite3_vfs *, const char *);
static void znsDlError(sqlite3_vfs *, int, char *);
static void (*znsDlSym(sqlite3_vfs *, void *, const char *))(void);
static void znsDlClose(sqlite3_vfs *, void *);
static int znsRandomness(sqlite3_vfs *, int, char *);
static int znsSleep(sqlite3_vfs *, int);
static int znsCurrentTime(sqlite3_vfs *, double *);
static int znsGetLastError(sqlite3_vfs *, int, char *);
static int znsCurrentTimeInt64(sqlite3_vfs *, sqlite3_int64 *);
// static int znsSetSystemCall(sqlite3_vfs*, const char*, sqlite3_syscall_ptr);
// static sqlite3_syscall_ptr znsGetSystemCall(sqlite3_vfs*, const char*);
// static const char *znsNextSystemCall(sqlite3_vfs*, const char*);

/*
** The zns_file_methods object specifies the methods for
** the ZNS SSD VFS implementation
*/
static const sqlite3_io_methods zns_file_methods = {
    3,                        /* iVersion */
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

/*
** Reset a ZNS zone using the BLKRESETZONE ioctl.
*/
static int resetZnsZone(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;
    int fd = -1; // Initialize fd to -1
    int rc = SQLITE_OK;
    // struct stat st; // stat is not needed if only resetting based on sector 0
    struct blk_zone_range range = {0};

    if (!p->isZnsWal || !p->pReal || !p->zPath) // Check necessary pointers
        return SQLITE_OK;

    /* Directly open the zone file path to get a file descriptor */
    fd = open(p->zPath, O_RDWR);
    if (fd < 0)
    {
        fprintf(stderr, "ZNS VFS Error: Could not open zone file %s directly for reset: %s\n", p->zPath, strerror(errno));
        return SQLITE_IOERR_ACCESS; // Indicate failure to access for reset
    }

    /* For BLKRESETZONE, range usually just needs the start sector (0). */
    range.sector = 0;
    range.nr_sectors = 0; // Often ignored for zonefs files, set to 0

    /* Issue the correct ioctl for ZNS zone reset */
    if (ioctl(fd, BLKRESETZONE, &range) != 0)
    {
        fprintf(stderr, "ZNS VFS Error: BLKRESETZONE failed for fd %d (%s): %s\n", fd, p->zPath, strerror(errno));
        rc = SQLITE_IOERR_TRUNCATE; // Map the error appropriately
        // Fall through to close fd
    }
    else
    {
        rc = SQLITE_OK;
        // fprintf(stderr, "ZNS VFS DEBUG: Successfully reset zone for %s\n", p->zPath);
    }

    // reset_zone_cleanup: // Label no longer strictly needed here
    if (fd >= 0)
    {
        close(fd); // Always close the fd we opened
    }

    return rc;
}

/* --- Helper: Ensure buffer capacity --- */
static int znsEnsureBuffer(zns_file *p, sqlite3_int64 needed)
{
    if (needed > p->nAlloc)
    {
        // Calculate new size: at least needed, at least double current, at least 4k, align to 1k
        sqlite3_int64 newAlloc = needed;
        if (newAlloc < p->nAlloc * 2)
            newAlloc = p->nAlloc * 2;
        if (newAlloc < 4096)
            newAlloc = 4096;
        newAlloc = (newAlloc + 1023) & ~1023LL; // Align up to 1KB boundary

        // fprintf(stderr, "ZNS VFS DEBUG: Reallocating buffer from %lld to %lld bytes (needed %lld)\n", p->nAlloc, newAlloc, needed);

        unsigned char *pNew = sqlite3_realloc(p->pBuffer, (size_t)newAlloc); // Use size_t for alloc size
        if (!pNew)
        {
            fprintf(stderr, "ZNS VFS Error: Failed to reallocate write buffer to %lld bytes\n", newAlloc);
            return SQLITE_NOMEM;
        }
        p->pBuffer = pNew;
        p->nAlloc = newAlloc;
    }
    return SQLITE_OK;
}

/* --- Helper: Flush buffer content --- */
static int znsFlushBuffer(zns_file *p)
{
    int rc = SQLITE_OK;
    // Check if it's a ZNS WAL, buffer exists, and there's data to flush
    if (!p->isZnsWal || !p->pBuffer || p->nBuffer <= p->iFlushed)
    {
        return SQLITE_OK;
    }

    sqlite3_int64 writeAmt = p->nBuffer - p->iFlushed;
    sqlite3_int64 writeOfst = p->iFlushed;
    unsigned char *writePtr = p->pBuffer + p->iFlushed;

    if (writeAmt <= 0)
        return SQLITE_OK; // Should be caught above, but double check

    // fprintf(stderr, "ZNS VFS DEBUG: Flushing %lld bytes from offset %lld to %s\n", writeAmt, writeOfst, p->zPath);

    rc = p->pReal->pMethods->xWrite(p->pReal, writePtr, (int)writeAmt, writeOfst);
    if (rc == SQLITE_OK)
    {
        p->iFlushed = p->nBuffer;
        // fprintf(stderr, "ZNS VFS DEBUG: Flush successful. iFlushed = %lld\n", p->iFlushed);
    }
    else
    {
        fprintf(stderr, "ZNS VFS Error: Failed to flush buffer (xWrite rc=%d, amt=%lld, ofst=%lld, file=%s, errno=%d %s)\n",
                rc, writeAmt, writeOfst, p->zPath, errno, strerror(errno));
        // How to handle flush failure? The buffer state is now inconsistent with disk.
        // Maybe attempt recovery on next sync? For now, just report error.
    }
    return rc;
}

/*
** Close a zns-file.
*/
static int znsClose(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;
    int rc = SQLITE_OK;
    const char *zBaseName = NULL;
    int i;

    // fprintf(stderr, "ZNS VFS DEBUG: Closing file %s (isZnsWal=%d)\n", p->zPath ? p->zPath : "<null path>", p->isZnsWal);

    /* Flush any remaining buffer content for ZNS WAL (Optional, usually sync before close) */
    // if (p->isZnsWal && p->pBuffer && p->nBuffer > p->iFlushed) {
    //     fprintf(stderr, "ZNS VFS Warning: Flushing buffer on close for %s\n", p->zPath);
    //     znsFlushBuffer(p); // Attempt flush, ignore error?
    // }

    /* Free the buffer if allocated */
    if (p->isZnsWal && p->pBuffer)
    {
        sqlite3_free(p->pBuffer);
        p->pBuffer = NULL;
        p->nBuffer = 0;
        p->nAlloc = 0;
        p->iFlushed = 0;
        // Mutex free if used:
        // if (p->pBufMutex) {
        //     sqlite3_mutex_free(p->pBufMutex);
        //     p->pBufMutex = NULL;
        // }
    }

    /* If this was a ZNS WAL file, mark the zone as free in the manager */
    if (p->isZnsWal && zoneManager.zZnsPath && zoneManager.aZoneFiles && p->zPath)
    {
        // Extract the zone file name (not the original WAL name)
        zBaseName = strrchr(p->zPath, '/');
        zBaseName = zBaseName ? zBaseName + 1 : p->zPath; // Should be like "0001"

        sqlite3_mutex_enter(zoneManager.mutex);
        for (i = 0; i < zoneManager.nZones; i++)
        {
            // Compare against the actual zone file path stored in the manager
            if (zoneManager.aZoneFiles[i] && strcmp(p->zPath, zoneManager.aZoneFiles[i]) == 0)
            {
                // fprintf(stderr, "ZNS VFS DEBUG: Releasing zone %s (index %d) in manager.\n", p->zPath, i);
                if (zoneManager.aZoneState[i] == 1) // Only free if currently allocated
                {
                    zoneManager.aZoneState[i] = 0; /* Mark as Free */
                    sqlite3_free(zoneManager.aWalNames[i]);
                    zoneManager.aWalNames[i] = NULL;
                }
                else
                {
                    // fprintf(stderr, "ZNS VFS Warning: Zone %s (index %d) was already free when closing.\n", p->zPath, i);
                }
                break;
            }
        }
        sqlite3_mutex_leave(zoneManager.mutex);
    }

    /* Close the underlying file handle */
    if (p->pReal && p->pReal->pMethods)
    {
        rc = p->pReal->pMethods->xClose(p->pReal);
    }
    else
    {
        // Underlying file might not have been opened successfully
        rc = SQLITE_OK; // Or return an error if p->pReal should always be valid here?
    }

    /* Free the filename copy */
    sqlite3_free(p->zPath);
    p->zPath = NULL;

    // Zero out the structure at the end (optional)
    // memset(p, 0, sizeof(zns_file));

    return rc;
}

/*
** Read data from a zns-file. (Pass through - reads hit flushed data)
*/
static int znsRead(
    sqlite3_file *pFile,
    void *zBuf,
    int iAmt,
    sqlite3_int64 iOfst)
{
    zns_file *p = (zns_file *)pFile;
    int rc;

    // fprintf(stderr, "ZNS VFS DEBUG: Reading %d bytes from offset %lld in %s (flushed=%lld)\n", iAmt, iOfst, p->zPath, p->iFlushed);

    /* Reads should only happen up to the flushed size for ZNS WAL */
    /* SQLite WAL reader should normally not read beyond the last commit point (which implies flushed data) */
    /* However, to be safe, let's just pass through. The underlying file system */
    /* should handle reads correctly up to the actual write pointer. */
    if (p->isZnsWal && iOfst + iAmt > p->iFlushed)
    {
        // This might happen if reading data written in the current uncommitted Tx.
        // Allowing pass-through read seems necessary.
        // fprintf(stderr, "ZNS VFS Warning: Read request (%lld + %d) potentially beyond flushed size (%lld) for %s. Passing through.\n", iOfst, iAmt, p->iFlushed, p->zPath);
    }

    rc = p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
    // if( rc!=SQLITE_OK && rc!=SQLITE_IOERR_SHORT_READ ){
    //     fprintf(stderr, "ZNS VFS Error: Underlying xRead failed (rc=%d, amt=%d, ofst=%lld, file=%s, errno=%d %s)\n", rc, iAmt, iOfst, p->zPath, errno, strerror(errno));
    // }
    return rc;
}

/*
** Write data to a zns-file. (Uses Buffering for ZNS WAL)
*/
static int znsWrite(
    sqlite3_file *pFile,
    const void *zBuf,
    int iAmt,
    sqlite3_int64 iOfst)
{
    zns_file *p = (zns_file *)pFile;
    int rc = SQLITE_OK;

    /* For ZNS WAL files, use the buffer */
    if (p->isZnsWal)
    {
        // Optional: Add mutex for buffer access
        // sqlite3_mutex_enter(p->pBufMutex);

        // fprintf(stderr, "ZNS VFS DEBUG: Write request %d bytes at offset %lld to %s (buffer size %lld)\n", iAmt, iOfst, p->zPath, p->nBuffer);

        /* Ensure buffer has enough capacity */
        rc = znsEnsureBuffer(p, iOfst + iAmt);
        if (rc != SQLITE_OK)
        {
            // sqlite3_mutex_leave(p->pBufMutex);
            return rc;
        }

        /* Check for sequential append or valid overwrite *within the buffer* */
        /* A write offset must be <= the current buffer size */
        if (iOfst > p->nBuffer)
        {
            fprintf(stderr, "ZNS VFS Error: Attempted non-sequential write (gap) to ZNS WAL buffer. Offset %lld > Buffer Size %lld in %s\n", iOfst, p->nBuffer, p->zPath);
            // sqlite3_mutex_leave(p->pBufMutex);
            return SQLITE_IOERR_WRITE; // Disallow gaps
        }

        /* Copy data into the buffer */
        memcpy(p->pBuffer + iOfst, zBuf, iAmt);

        /* Update logical buffer size if appending */
        if (iOfst + iAmt > p->nBuffer)
        {
            p->nBuffer = iOfst + iAmt;
        }

        // fprintf(stderr, "ZNS VFS DEBUG: Buffered write successful. New buffer size %lld for %s\n", p->nBuffer, p->zPath);

        // sqlite3_mutex_leave(p->pBufMutex);
        return SQLITE_OK; // Data is buffered, actual write happens on sync/flush
    }

    /* For non-ZNS files, pass through to the real VFS */
    // fprintf(stderr, "ZNS VFS DEBUG: Passing through write %d bytes at offset %lld to %s\n", iAmt, iOfst, p->zPath);
    rc = p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
    // if( rc!=SQLITE_OK ){
    //     fprintf(stderr, "ZNS VFS Error: Underlying passthrough xWrite failed (rc=%d, amt=%d, ofst=%lld, file=%s, errno=%d %s)\n", rc, iAmt, iOfst, p->zPath, errno, strerror(errno));
    // }
    return rc;
}

/*
** Truncate a zns-file.
*/
static int znsTruncate(sqlite3_file *pFile, sqlite3_int64 size)
{
    zns_file *p = (zns_file *)pFile;
    int rc = SQLITE_OK;

    // fprintf(stderr, "ZNS VFS DEBUG: Truncate request size=%lld for %s (isZnsWal=%d)\n", size, p->zPath, p->isZnsWal);

    /* For ZNS WAL files, truncate(0) means reset the zone and buffer */
    if (p->isZnsWal && size == 0)
    {
        fprintf(stderr, "ZNS VFS INFO: Truncate(0) called for ZNS WAL %s. Resetting zone and buffer.\n", p->zPath);

        // Reset buffer state
        // sqlite3_mutex_enter(p->pBufMutex); // Optional mutex
        p->nBuffer = 0;
        p->iFlushed = 0;
        // Optionally free and reallocate buffer to minimum size, or just keep allocated buffer?
        // Keeping it might be slightly faster if immediately reused.
        // sqlite3_free(p->pBuffer); p->pBuffer = NULL; p->nAlloc = 0;
        // sqlite3_mutex_leave(p->pBufMutex);

        // Reset the physical zone
        rc = resetZnsZone(pFile);
        if (rc != SQLITE_OK)
        {
            fprintf(stderr, "ZNS VFS Error: resetZnsZone failed during Truncate(0) for %s (rc=%d)\n", p->zPath, rc);
            // Even if reset fails, buffer is reset. Should we restore buffer state? Probably not.
        }
        return rc;
    }

    /* Truncating to non-zero size for ZNS WAL */
    if (p->isZnsWal)
    {
        fprintf(stderr, "ZNS VFS Warning: Truncate to non-zero size (%lld) requested for ZNS WAL file %s. Operation ignored.\n", size, p->zPath);
        // Cannot truncate a zone to arbitrary size. Best to ignore and return OK.
        return SQLITE_OK;
    }

    /* Pass through for non-ZNS files */
    return p->pReal->pMethods->xTruncate(p->pReal, size);
}

/*
** Sync a zns-file. (Flushes buffer for ZNS WAL)
*/
static int znsSync(sqlite3_file *pFile, int flags)
{
    zns_file *p = (zns_file *)pFile;
    int rc = SQLITE_OK;

    // fprintf(stderr, "ZNS VFS DEBUG: Sync request flags=0x%x for %s (isZnsWal=%d)\n", flags, p->zPath, p->isZnsWal);

    /* For ZNS WAL files, flush the buffer then sync the underlying file */
    if (p->isZnsWal)
    {
        // sqlite3_mutex_enter(p->pBufMutex); // Optional mutex
        rc = znsFlushBuffer(p); // Write buffered data to disk first
        if (rc == SQLITE_OK)
        {
            // Only call underlying sync if flush was successful
            // fprintf(stderr, "ZNS VFS DEBUG: Flushing successful for %s, calling underlying sync.\n", p->zPath);
            rc = p->pReal->pMethods->xSync(p->pReal, flags);
            if (rc != SQLITE_OK)
            {
                fprintf(stderr, "ZNS VFS Error: Underlying xSync failed for %s (rc=%d, errno=%d %s)\n", p->zPath, rc, errno, strerror(errno));
            }
        }
        else
        {
            fprintf(stderr, "ZNS VFS Error: Flush buffer failed during sync for %s (rc=%d). Sync aborted.\n", p->zPath, rc);
        }
        // sqlite3_mutex_leave(p->pBufMutex);
        return rc;
    }

    /* Pass through for non-ZNS files */
    return p->pReal->pMethods->xSync(p->pReal, flags);
}

/*
** Get file size of a zns-file. (Returns buffered size for ZNS WAL)
*/
static int znsFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize)
{
    zns_file *p = (zns_file *)pFile;

    /* For ZNS WAL files, return the logical (buffered) size */
    if (p->isZnsWal)
    {
        // sqlite3_mutex_enter(p->pBufMutex); // Optional mutex
        *pSize = p->nBuffer;
        // sqlite3_mutex_leave(p->pBufMutex);
        // fprintf(stderr, "ZNS VFS DEBUG: Reporting file size %lld for ZNS WAL %s\n", *pSize, p->zPath);
        return SQLITE_OK;
    }

    /* Pass through for non-ZNS files */
    return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}

/* --- Pass-through methods --- */

static int znsLock(sqlite3_file *pFile, int eLock)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xLock(p->pReal, eLock);
}
static int znsUnlock(sqlite3_file *pFile, int eLock)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}
static int znsCheckReservedLock(sqlite3_file *pFile, int *pResOut)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
}
static int znsFileControl(sqlite3_file *pFile, int op, void *pArg)
{
    zns_file *p = (zns_file *)pFile;
    /* Intercept FCNTL_FILE_DESCRIPTOR for resetZnsZone fallback? */
    // if (op == SQLITE_FCNTL_FILE_DESCRIPTOR) {
    //     // Let the original VFS handle it if it can
    // }
    /* Intercept specific controls if needed, e.g., ZNS specific hints */
    // if (p->isZnsWal && op == XYZ_ZNS_HINT) { ... }
    return p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
}
static int znsSectorSize(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xSectorSize(p->pReal);
}
static int znsDeviceCharacteristics(sqlite3_file *pFile)
{
    zns_file *p = (zns_file *)pFile;
    int characteristics = p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
    /* Advertise sequential-only for ZNS WAL files? */
    if (p->isZnsWal)
    {
        /* SQLITE_IOCAP_SEQUENTIAL tells SQLite WAL not to bother with checksum rewrites,
           which simplifies things but might subtly change behavior or assumptions.
           Let's NOT advertise sequential for now, rely on buffering to handle rewrites. */
        // characteristics |= SQLITE_IOCAP_SEQUENTIAL;

        /* ZNS zones might inherently have power-safe overwrite properties IF writes */
        /* are guaranteed to land sequentially and zonefs/device handles metadata safely.*/
        /* It's safer NOT to assume this unless the underlying hardware guarantees it. */
        // characteristics |= SQLITE_IOCAP_POWERSAFE_OVERWRITE;
    }
    return characteristics;
}

/* --- SHM methods (Pass-through, SHM is not on ZNS) --- */

static int znsShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend, void volatile **pp)
{
    zns_file *p = (zns_file *)pFile;
    return p->pReal->pMethods->xShmMap(p->pReal, iPg, pgsz, bExtend, pp);
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

/* --- Fetch/Unfetch (Pass-through, related to Win32 specific VFS usually) --- */

static int znsFetch(sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt, void **pp)
{
    zns_file *p = (zns_file *)pFile;
    if (p->pReal->pMethods->iVersion >= 3 && p->pReal->pMethods->xFetch)
    {
        return p->pReal->pMethods->xFetch(p->pReal, iOfst, iAmt, pp);
    }
    *pp = 0;
    return SQLITE_OK; /* Or maybe SQLITE_IOERR? */
}
static int znsUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pData)
{
    zns_file *p = (zns_file *)pFile;
    if (p->pReal->pMethods->iVersion >= 3 && p->pReal->pMethods->xUnfetch)
    {
        return p->pReal->pMethods->xUnfetch(p->pReal, iOfst, pData);
    }
    return SQLITE_OK;
}

/* --- VFS Method Implementations --- */

/*
** Open a file handle.
** Handles redirection of WAL files to zone files if ZNS mode is enabled.
*/
static int znsOpen(
    sqlite3_vfs *pVfs,
    const char *zOrigName, /* Original name passed by SQLite */
    sqlite3_file *pFile,   /* The file handle structure to fill */
    int flags,             /* Input flags */
    int *pOutFlags         /* Output flags */
)
{
    zns_file *p = (zns_file *)pFile;
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData; // Get our impl data
    int rc = SQLITE_OK;
    const char *zNameToOpen = zOrigName;
    char *zZoneFile = NULL; // Full path to assigned zone file
    int isWal = 0;
    int isZnsWal = 0;
    int zoneIndex = -1;

    /* Ensure pOrigVfs is set */
    if (!pOrigVfs)
        pOrigVfs = pZnsVfs->pRealVfs;
    if (!pOrigVfs)
        return SQLITE_ERROR; // Should not happen if init was ok

    /* Determine if this is potentially a WAL file to be handled by ZNS */
    if (zOrigName && (flags & SQLITE_OPEN_WAL) && sqlite3WalUseZnsSsd_internal())
    {
        isWal = 1;
        // fprintf(stderr, "ZNS VFS DEBUG: Attempting to open potential WAL file: %s\n", zOrigName);
        const char *currentZnsPath = sqlite3WalGetZnsSsdPath_internal(); // 경로도 _internal 사용
        if (!currentZnsPath)
        { /* 오류 처리 */
            return SQLITE_ERROR;
        }
        zZoneFile = znsGetFreeZoneFile(zOrigName, currentZnsPath); // 경로 전달

        if (zZoneFile)
        {
            // Found/allocated a zone file, use its path for opening
            zNameToOpen = zZoneFile;
            isZnsWal = 1;
            // Find the index of the allocated zone for cleanup on error
            sqlite3_mutex_enter(zoneManager.mutex);
            for (int i = 0; i < zoneManager.nZones; i++)
            {
                if (zoneManager.aZoneFiles[i] == zZoneFile)
                { // Pointer comparison is safe here
                    zoneIndex = i;
                    break;
                }
            }
            sqlite3_mutex_leave(zoneManager.mutex);
            assert(zoneIndex != -1); // Should have found the index

            fprintf(stderr, "ZNS VFS INFO: Mapping WAL %s to Zone %s (Index %d)\n", zOrigName, zNameToOpen, zoneIndex);
        }
        else
        {
            // No free zone available or zone manager init failed
            fprintf(stderr, "ZNS VFS Error: No free zone available or manager uninitialized for WAL %s\n", zOrigName);
            return SQLITE_FULL; // Indicate resource exhaustion
        }
    }

    /* Initialize the zns_file structure */
    memset(p, 0, sizeof(zns_file));                              // Zero out the whole struct first
    p->pReal = (sqlite3_file *)(((char *)p) + sizeof(zns_file)); // Real struct follows ours
    p->isWal = isWal;
    p->isZnsWal = isZnsWal;
    // Buffer fields (pBuffer, nBuffer, nAlloc, iFlushed) are zeroed by memset

    /* Modify flags for zonefs: Cannot create, cannot delete on close */
    int modifiedFlags = flags;
    if (isZnsWal)
    {
        // Zone files must already exist (created by zonefs setup)
        modifiedFlags &= ~(SQLITE_OPEN_CREATE | SQLITE_OPEN_DELETEONCLOSE);
        // Ensure we open read-write if it's a ZNS WAL, regardless of input flags?
        // SQLite usually opens WAL read-write anyway. Let's keep original RDWR flags.
        // modifiedFlags |= SQLITE_OPEN_READWRITE;
    }

    /* Open the underlying file using the original VFS */
    rc = pZnsVfs->pRealVfs->xOpen(pZnsVfs->pRealVfs, zNameToOpen, p->pReal, modifiedFlags, pOutFlags);

    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS VFS Error: Underlying xOpen failed for %s (rc=%d, name=%s, flags=0x%x)\n",
                isZnsWal ? "zone file" : "file", rc, zNameToOpen, modifiedFlags);
        if (isZnsWal && zZoneFile && zoneIndex != -1)
        {
            /* Open failed, release the allocated zone */
            // fprintf(stderr, "ZNS VFS DEBUG: Releasing zone %d due to xOpen failure.\n", zoneIndex);
            sqlite3_mutex_enter(zoneManager.mutex);
            if (zoneManager.aZoneState[zoneIndex] == 1 && /* check if still allocated by us */
                zoneManager.aWalNames[zoneIndex] != NULL) /* sanity check */
            {
                zoneManager.aZoneState[zoneIndex] = 0; /* Mark as Free */
                sqlite3_free(zoneManager.aWalNames[zoneIndex]);
                zoneManager.aWalNames[zoneIndex] = NULL;
            }
            sqlite3_mutex_leave(zoneManager.mutex);
        }
        return rc;
    }

    /* Store the actual path opened (could be original or zone file path) */
    p->zPath = sqlite3_mprintf("%s", zNameToOpen);
    if (!p->zPath)
    {
        p->pReal->pMethods->xClose(p->pReal); // Close the opened file
        if (isZnsWal && zZoneFile && zoneIndex != -1)
        {
            // Release zone
            sqlite3_mutex_enter(zoneManager.mutex);
            if (zoneManager.aZoneState[zoneIndex] == 1 && zoneManager.aWalNames[zoneIndex] != NULL)
            {
                zoneManager.aZoneState[zoneIndex] = 0;
                sqlite3_free(zoneManager.aWalNames[zoneIndex]);
                zoneManager.aWalNames[zoneIndex] = NULL;
            }
            sqlite3_mutex_leave(zoneManager.mutex);
        }
        return SQLITE_NOMEM;
    }

    /* Get initial flushed size (important if reopening an existing zone file) */
    if (p->isZnsWal)
    {
        rc = p->pReal->pMethods->xFileSize(p->pReal, &p->iFlushed);
        if (rc != SQLITE_OK)
        {
            fprintf(stderr, "ZNS VFS Error: Failed to get initial file size for %s (rc=%d)\n", p->zPath, rc);
            p->pReal->pMethods->xClose(p->pReal);
            sqlite3_free(p->zPath);
            // Release zone if allocated
            if (isZnsWal && zZoneFile && zoneIndex != -1)
            {
                sqlite3_mutex_enter(zoneManager.mutex);
                if (zoneManager.aZoneState[zoneIndex] == 1 && zoneManager.aWalNames[zoneIndex] != NULL)
                {
                    zoneManager.aZoneState[zoneIndex] = 0;
                    sqlite3_free(zoneManager.aWalNames[zoneIndex]);
                    zoneManager.aWalNames[zoneIndex] = NULL;
                }
                sqlite3_mutex_leave(zoneManager.mutex);
            }
            return rc;
        }
        p->nBuffer = p->iFlushed; // Initially, buffered size equals flushed size
        // fprintf(stderr, "ZNS VFS DEBUG: Opened ZNS WAL %s. Initial flushed/buffered size: %lld\n", p->zPath, p->nBuffer);
    }

    /* Set up the zns-file methods */
    p->base.pMethods = &zns_file_methods;

    return SQLITE_OK;
}

/*
** Delete the file located at zPath. For ZNS WAL files, this means resetting
** the corresponding zone and marking it as free.
*/
static int znsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    const char *zBaseName;
    int i;

    /* Check if it's a WAL file potentially handled by ZNS */
    /* Use sqlite3_uri_boolean to check for "-wal" suffix robustly? No, simple check is fine. */
    if (zPath && sqlite3WalUseZnsSsd_internal())
    {
        int nPath = strlen(zPath);
        if (nPath > 4 && sqlite3_strnicmp(&zPath[nPath - 4], "-wal", 4) == 0)
        {
            /* Extract the base WAL name (e.g., "database-wal") */
            zBaseName = strrchr(zPath, '/');
            zBaseName = zBaseName ? zBaseName + 1 : zPath;

            /* Check if the zone manager is initialized and seems valid */
            if (zoneManager.zZnsPath && zoneManager.aZoneFiles && zoneManager.aWalNames && zoneManager.mutex)
            {
                sqlite3_mutex_enter(zoneManager.mutex);
                int zoneFound = 0;
                int zoneIndex = -1;
                char *zZoneFilePath = NULL; // Store the path for reset

                /* Find the zone mapped to this WAL name */
                for (i = 0; i < zoneManager.nZones; i++)
                {
                    if (zoneManager.aZoneState[i] == 1 && zoneManager.aWalNames[i] &&
                        strcmp(zoneManager.aWalNames[i], zBaseName) == 0)
                    {
                        zoneIndex = i;
                        zoneFound = 1;
                        zZoneFilePath = zoneManager.aZoneFiles[i]; // Get the path *before* modifying state
                        break;
                    }
                }

                if (zoneFound && zZoneFilePath)
                {
                    fprintf(stderr, "ZNS VFS INFO: Deleting (Resetting) Zone %s for WAL %s\n",
                            zZoneFilePath, zBaseName);

                    /* Reset the zone. We need to open the zone file temporarily. */
                    int fd = -1;
                    int rc_reset = SQLITE_ERROR;
                    struct stat st;
                    struct blk_zone_range range = {0};

                    fd = open(zZoneFilePath, O_RDWR);
                    if (fd >= 0)
                    {
                        // No need for fstat, just set sector=0 for reset
                        range.sector = 0;
                        range.nr_sectors = 0; // Usually ignored
                        if (ioctl(fd, BLKRESETZONE, &range) == 0)
                        {
                            rc_reset = SQLITE_OK;
                            // fprintf(stderr, "ZNS VFS DEBUG: BLKRESETZONE successful for %s\n", zZoneFilePath);
                        }
                        else
                        {
                            fprintf(stderr, "ZNS VFS Error: BLKRESETZONE failed for %s: %s\n",
                                    zZoneFilePath, strerror(errno));
                            rc_reset = SQLITE_IOERR_DELETE; // Use specific delete error
                        }
                        close(fd);
                    }
                    else
                    {
                        fprintf(stderr, "ZNS VFS Error: Could not open zone file %s for reset: %s\n",
                                zZoneFilePath, strerror(errno));
                        rc_reset = SQLITE_IOERR_DELETE;
                    }

                    /* Update zone manager state regardless of reset success? Yes. */
                    /* The mapping is gone, even if reset failed. */
                    zoneManager.aZoneState[zoneIndex] = 0; /* Mark as Free */
                    sqlite3_free(zoneManager.aWalNames[zoneIndex]);
                    zoneManager.aWalNames[zoneIndex] = NULL;

                    sqlite3_mutex_leave(zoneManager.mutex);
                    /* Return OK if the conceptual delete (mapping removal) succeeded, */
                    /* even if physical reset failed. */
                    return SQLITE_OK;
                    // Alternatively, return rc_reset if strict error propagation is needed:
                    // return rc_reset;
                }
                else
                {
                    // fprintf(stderr, "ZNS VFS Warning: Delete called for WAL %s, but no allocated zone found.\n", zBaseName);
                }

                sqlite3_mutex_leave(zoneManager.mutex);
            }
            else
            {
                // fprintf(stderr, "ZNS VFS Warning: Delete called for potential WAL %s, but Zone Manager not ready.\n", zPath);
            }
        }
    }

    /* If not a ZNS WAL file or ZNS is disabled/uninitialized, pass through */
    // fprintf(stderr, "ZNS VFS DEBUG: Passing delete for %s to underlying VFS.\n", zPath);
    return pZnsVfs->pRealVfs->xDelete(pZnsVfs->pRealVfs, zPath, dirSync);
}

/*
** Test for access permissions.
** For ZNS WAL, check if a zone is allocated. Otherwise, passthrough.
*/
static int znsAccess(
    sqlite3_vfs *pVfs,
    const char *zPath,
    int flags,
    int *pResOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    const char *zBaseName;
    int i;

    /* Check if it's a WAL file potentially handled by ZNS */
    if (zPath && sqlite3WalUseZnsSsd_internal())
    {
        int nPath = strlen(zPath);
        if (nPath > 4 && sqlite3_strnicmp(&zPath[nPath - 4], "-wal", 4) == 0)
        {
            zBaseName = strrchr(zPath, '/');
            zBaseName = zBaseName ? zBaseName + 1 : zPath;

            /* Check if the zone manager is initialized */
            if (zoneManager.zZnsPath && zoneManager.aZoneFiles && zoneManager.aWalNames && zoneManager.mutex)
            {
                int zoneFound = 0;
                char *zZoneFilePath = NULL;
                sqlite3_mutex_enter(zoneManager.mutex);
                for (i = 0; i < zoneManager.nZones; i++)
                {
                    if (zoneManager.aZoneState[i] == 1 && zoneManager.aWalNames[i] &&
                        strcmp(zoneManager.aWalNames[i], zBaseName) == 0)
                    {
                        zoneFound = 1;
                        zZoneFilePath = zoneManager.aZoneFiles[i]; // Get path for potential check
                        break;
                    }
                }
                sqlite3_mutex_leave(zoneManager.mutex);

                if (zoneFound && zZoneFilePath)
                {
                    /* Zone mapping exists. Check actual file access on the zone file */
                    /* Pass the *zone file path* to the underlying access check */
                    // fprintf(stderr, "ZNS VFS DEBUG: Access check for ZNS WAL %s -> zone %s\n", zPath, zZoneFilePath);
                    return pZnsVfs->pRealVfs->xAccess(pZnsVfs->pRealVfs, zZoneFilePath, flags, pResOut);
                }
                else
                {
                    // No zone allocated for this name, so it doesn't exist in ZNS VFS terms
                    // fprintf(stderr, "ZNS VFS DEBUG: Access check for ZNS WAL %s: No zone allocated.\n", zPath);
                    *pResOut = 0; // Indicate not found
                    return SQLITE_OK;
                }
            }
            // else { fprintf(stderr, "ZNS VFS DEBUG: Access check for potential WAL %s, Zone Manager not ready.\n", zPath); }
        }
    }

    /* Fall back to the original VFS for non-ZNS files or if ZNS is disabled */
    // fprintf(stderr, "ZNS VFS DEBUG: Passing access check for %s to underlying VFS.\n", zPath);
    return pZnsVfs->pRealVfs->xAccess(pZnsVfs->pRealVfs, zPath, flags, pResOut);
}

/*
** Get the canonical path name
*/
static int znsFullPathname(
    sqlite3_vfs *pVfs,
    const char *zPath,
    int nPathOut,
    char *zPathOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    /* No redirection logic needed here, just pass through */
    return pZnsVfs->pRealVfs->xFullPathname(pZnsVfs->pRealVfs, zPath, nPathOut, zPathOut);
}

/* --- Dynamic Loading stubs (Pass-through) --- */
static void *znsDlOpen(sqlite3_vfs *pVfs, const char *zPath)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    return pZnsVfs->pRealVfs->xDlOpen(pZnsVfs->pRealVfs, zPath);
}
static void znsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    pZnsVfs->pRealVfs->xDlError(pZnsVfs->pRealVfs, nByte, zErrMsg);
}
static void (*znsDlSym(sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol))(void)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    return pZnsVfs->pRealVfs->xDlSym(pZnsVfs->pRealVfs, pHandle, zSymbol);
}
static void znsDlClose(sqlite3_vfs *pVfs, void *pHandle)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    pZnsVfs->pRealVfs->xDlClose(pZnsVfs->pRealVfs, pHandle);
}

/* --- Randomness, Sleep, Time (Pass-through) --- */
static int znsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    return pZnsVfs->pRealVfs->xRandomness(pZnsVfs->pRealVfs, nByte, zBufOut);
}
static int znsSleep(sqlite3_vfs *pVfs, int microsec)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    return pZnsVfs->pRealVfs->xSleep(pZnsVfs->pRealVfs, microsec);
}
static int znsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    return pZnsVfs->pRealVfs->xCurrentTime(pZnsVfs->pRealVfs, pTimeOut);
}
static int znsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    /* Check if the underlying VFS supports CurrentTimeInt64 */
    if (pZnsVfs->pRealVfs->iVersion >= 2 && pZnsVfs->pRealVfs->xCurrentTimeInt64)
    {
        return pZnsVfs->pRealVfs->xCurrentTimeInt64(pZnsVfs->pRealVfs, pTimeOut);
    }
    else
    {
        /* Fallback using xCurrentTime if Int64 version not available */
        double r;
        int rc = pZnsVfs->pRealVfs->xCurrentTime(pZnsVfs->pRealVfs, &r);
        *pTimeOut = (sqlite3_int64)(r * 86400000.0);
        return rc;
    }
}

static int znsGetLastError(sqlite3_vfs *pVfs, int iErrno, char *zErrstr)
{
    zns_vfs *pZnsVfs = (zns_vfs *)pVfs->pAppData;
    /* Check version before calling */
    if (pZnsVfs->pRealVfs->iVersion >= 1 && pZnsVfs->pRealVfs->xGetLastError)
    {
        return pZnsVfs->pRealVfs->xGetLastError(pZnsVfs->pRealVfs, iErrno, zErrstr);
    }
    /* Provide a generic message if the underlying VFS doesn't support it */
    if (zErrstr)
    {
        // Use a fixed reasonable size like 512 for the buffer length
        sqlite3_snprintf(512, zErrstr, "System call error number %d", iErrno);
    }
    return iErrno;
}

/* --- Zone Manager Functions --- */

/*
** Zone 관리자 초기화 함수
*/
static int znsZoneManagerInit(const char *zZnsPath)
{
    DIR *dir = NULL;
    struct dirent *entry;
    int nZones = 0;
    int i = 0;
    int rc = SQLITE_OK;
    char **aZoneFilesTemp = NULL; // Temporary arrays for allocation
    int *aZoneStateTemp = NULL;
    char **aWalNamesTemp = NULL;

    if (!zZnsPath || !zZnsPath[0])
        return SQLITE_ERROR;

    /* Already initialized and path matches? */
    sqlite3_mutex_enter(zoneManager.mutex); // Need mutex early for thread safety
    if (zoneManager.zZnsPath)
    {
        if (strcmp(zoneManager.zZnsPath, zZnsPath) == 0)
        {
            sqlite3_mutex_leave(zoneManager.mutex);
            return SQLITE_OK; // Already initialized with same path
        }
        else
        {
            // Path changed - need to destroy old and re-initialize
            fprintf(stderr, "ZNS VFS INFO: ZNS path changed. Re-initializing Zone Manager.\n");
            sqlite3_mutex_leave(zoneManager.mutex); // Leave before calling destroy
            znsZoneManagerDestroy();
            sqlite3_mutex_enter(zoneManager.mutex); // Re-acquire for init
        }
    }
    sqlite3_mutex_leave(zoneManager.mutex); // Leave before potentially long opendir/readdir

    fprintf(stderr, "ZNS VFS INFO: Initializing Zone Manager for path: %s\n", zZnsPath);

    /* Open ZNS path directory */
    dir = opendir(zZnsPath);
    if (!dir)
    {
        fprintf(stderr, "ZNS VFS Error: Cannot open directory %s: %s\n", zZnsPath, strerror(errno));
        return SQLITE_CANTOPEN;
    }

    /* Count potential zone files matching the pattern */
    while ((entry = readdir(dir)) != NULL)
    {
        if (entry->d_type == DT_REG || entry->d_type == DT_UNKNOWN) // Check UNKNOWN too (some fs)
        {
            unsigned int zoneNum; // Use unsigned for sscanf
            if (sscanf(entry->d_name, ZONEFS_SEQ_FILE_PATTERN, &zoneNum) == 1)
            {
                nZones++;
            }
        }
    }
    fprintf(stderr, "ZNS VFS INFO: Found %d potential zone files matching pattern '%s'.\n", nZones, ZONEFS_SEQ_FILE_PATTERN);
    if (nZones == 0)
    {
        fprintf(stderr, "ZNS VFS Warning: No zone files found in %s.\n", zZnsPath);
        // Proceed with 0 zones, manager will be mostly inactive
    }

    rewinddir(dir); // Reset directory stream to read names again

    /* Allocate memory (use temporary pointers first) */
    /* Note: Using sqlite3_*alloc ensures SQLite knows about this memory if tracking */
    aZoneStateTemp = sqlite3_malloc(sizeof(int) * nZones);
    aZoneFilesTemp = sqlite3_malloc(sizeof(char *) * nZones);
    aWalNamesTemp = sqlite3_malloc(sizeof(char *) * nZones);

    if (nZones > 0 && (!aZoneStateTemp || !aZoneFilesTemp || !aWalNamesTemp))
    {
        fprintf(stderr, "ZNS VFS Error: Failed to allocate memory for Zone Manager arrays (nZones=%d).\n", nZones);
        rc = SQLITE_NOMEM;
        goto init_error_cleanup;
    }

    /* Zero out the pointer arrays */
    if (nZones > 0)
    {
        memset(aZoneStateTemp, 0, sizeof(int) * nZones); // Initialize state to Free
        memset(aZoneFilesTemp, 0, sizeof(char *) * nZones);
        memset(aWalNamesTemp, 0, sizeof(char *) * nZones);
    }

    /* Store zone file paths */
    i = 0;
    while ((entry = readdir(dir)) != NULL && i < nZones)
    {
        if (entry->d_type == DT_REG || entry->d_type == DT_UNKNOWN)
        {
            unsigned int zoneNum;
            if (sscanf(entry->d_name, ZONEFS_SEQ_FILE_PATTERN, &zoneNum) == 1)
            {
                // Allocate full path string
                aZoneFilesTemp[i] = sqlite3_mprintf("%s/%s", zZnsPath, entry->d_name);
                if (!aZoneFilesTemp[i])
                {
                    fprintf(stderr, "ZNS VFS Error: Failed to allocate memory for zone file path %s/%s.\n", zZnsPath, entry->d_name);
                    rc = SQLITE_NOMEM;
                    // Need to free already allocated path strings before bailing out
                    for (int j = 0; j < i; j++)
                    {
                        sqlite3_free(aZoneFilesTemp[j]);
                    }
                    goto init_error_cleanup;
                }
                i++;
            }
        }
    }
    closedir(dir); // Close directory stream now
    dir = NULL;    // Mark as closed

    /* Now, acquire mutex and update the global structure */
    sqlite3_mutex_enter(zoneManager.mutex);
    if (zoneManager.zZnsPath)
    { // Check if another thread initialized in the meantime
        sqlite3_mutex_leave(zoneManager.mutex);
        fprintf(stderr, "ZNS VFS Warning: Zone Manager initialized by another thread concurrently.\n");
        // Free temporary allocations
        sqlite3_free(aZoneStateTemp);
        if (aZoneFilesTemp)
        {
            for (int k = 0; k < i; k++)
                sqlite3_free(aZoneFilesTemp[k]); // Free paths allocated in this attempt
            sqlite3_free(aZoneFilesTemp);
        }
        sqlite3_free(aWalNamesTemp);
        return SQLITE_OK; // Assume the other thread succeeded
    }

    /* Assign allocated resources to the global manager */
    zoneManager.zZnsPath = sqlite3_mprintf("%s", zZnsPath); // Duplicate path string
    if (!zoneManager.zZnsPath)
    {
        rc = SQLITE_NOMEM; // Failed to copy path string
    }
    else
    {
        zoneManager.nZones = nZones;
        zoneManager.aZoneState = aZoneStateTemp;
        zoneManager.aZoneFiles = aZoneFilesTemp;
        zoneManager.aWalNames = aWalNamesTemp;
        // Ensure mutex is allocated if not already (should be done first ideally)
        if (!zoneManager.mutex)
        {
            zoneManager.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
            if (!zoneManager.mutex)
                rc = SQLITE_NOMEM;
        }
    }

    if (rc != SQLITE_OK)
    {
        // Cleanup if assignment failed
        sqlite3_free(zoneManager.zZnsPath);
        zoneManager.zZnsPath = NULL;
        sqlite3_free(aZoneStateTemp); // Free temps as they weren't assigned
        if (aZoneFilesTemp)
        {
            for (int k = 0; k < i; k++)
                sqlite3_free(aZoneFilesTemp[k]);
            sqlite3_free(aZoneFilesTemp);
        }
        sqlite3_free(aWalNamesTemp);
        if (zoneManager.mutex)
        {
            sqlite3_mutex_free(zoneManager.mutex);
            zoneManager.mutex = NULL;
        }
        memset(&zoneManager, 0, sizeof(zoneManager)); // Clear manager struct
        sqlite3_mutex_leave(zoneManager.mutex);
        fprintf(stderr, "ZNS VFS Error: Failed during final assignment in Zone Manager Init (rc=%d).\n", rc);
        return rc;
    }

    sqlite3_mutex_leave(zoneManager.mutex);
    fprintf(stderr, "ZNS VFS INFO: Zone Manager Initialized successfully with %d zones for %s.\n", zoneManager.nZones, zoneManager.zZnsPath);
    return SQLITE_OK;

init_error_cleanup:
    if (dir)
        closedir(dir);
    // Free temporary arrays if allocation failed partway
    sqlite3_free(aZoneStateTemp);
    sqlite3_free(aZoneFilesTemp); // Note: contained paths are not allocated if this failed early
    sqlite3_free(aWalNamesTemp);
    return rc;
}

/*
** Zone 관리자 해제 함수
*/
static void znsZoneManagerDestroy(void)
{
    int i;

    if (!zoneManager.mutex)
        return; // Not initialized or already destroyed

    sqlite3_mutex_enter(zoneManager.mutex);

    if (!zoneManager.zZnsPath)
    { // Check again inside mutex
        sqlite3_mutex_leave(zoneManager.mutex);
        return;
    }

    fprintf(stderr, "ZNS VFS INFO: Destroying Zone Manager for path: %s\n", zoneManager.zZnsPath);

    sqlite3_free(zoneManager.zZnsPath);
    zoneManager.zZnsPath = NULL;
    sqlite3_free(zoneManager.aZoneState);
    zoneManager.aZoneState = NULL;

    if (zoneManager.aZoneFiles)
    {
        for (i = 0; i < zoneManager.nZones; i++)
        {
            sqlite3_free(zoneManager.aZoneFiles[i]); // Free individual path strings
        }
        sqlite3_free(zoneManager.aZoneFiles); // Free the array of pointers
        zoneManager.aZoneFiles = NULL;
    }

    if (zoneManager.aWalNames)
    {
        for (i = 0; i < zoneManager.nZones; i++)
        {
            sqlite3_free(zoneManager.aWalNames[i]); // Free individual WAL name strings
        }
        sqlite3_free(zoneManager.aWalNames); // Free the array of pointers
        zoneManager.aWalNames = NULL;
    }

    zoneManager.nZones = 0;
    sqlite3_mutex *pMutexToFree = zoneManager.mutex; // Hold mutex pointer
    zoneManager.mutex = NULL;                        // Mark as destroyed inside lock

    sqlite3_mutex_leave(pMutexToFree); // Leave mutex BEFORE freeing it
    sqlite3_mutex_free(pMutexToFree);  // Free the mutex itself

    fprintf(stderr, "ZNS VFS INFO: Zone Manager Destroyed.\n");
}

/*
** 사용 가능한 zone 파일 찾고 할당하기
** zWalName: SQLite가 전달한 원본 WAL 파일 경로 (e.g., /path/to/db-wal)
** Returns: Full path to the allocated zone file (e.g., /zonefs/mount/0001)
**          or NULL if none available or error.
*/
static char *znsGetFreeZoneFile(const char *zWalName, const char *currentZnsPath)
{
    int i;
    char *zZoneFile = NULL;
    const char *zBaseName; // WAL 파일의 기본 이름 (e.g., "db-wal")

    /* Ensure manager is initialized for the current path */
    const char *currentZnsPath = sqlite3WalGetZnsSsdPath();
    if (!currentZnsPath || znsZoneManagerInit(currentZnsPath) != SQLITE_OK)
    {
        fprintf(stderr, "ZNS VFS Error: Zone Manager not initialized or init failed in znsGetFreeZoneFile.\n");
        return NULL;
    }
    // Re-check after init attempt (might have failed)
    if (!zoneManager.mutex || !zoneManager.zZnsPath || !zoneManager.aZoneFiles)
    {
        fprintf(stderr, "ZNS VFS Error: Zone Manager unavailable after init attempt.\n");
        return NULL;
    }

    /* Extract base WAL name (the part after the last '/') */
    zBaseName = strrchr(zWalName, '/');
    zBaseName = zBaseName ? zBaseName + 1 : zWalName;

    sqlite3_mutex_enter(zoneManager.mutex);

    /* 1. Check if a zone is already allocated for this specific WAL base name */
    for (i = 0; i < zoneManager.nZones; i++)
    {
        if (zoneManager.aZoneState[i] == 1 && zoneManager.aWalNames[i] &&
            strcmp(zoneManager.aWalNames[i], zBaseName) == 0)
        {
            zZoneFile = zoneManager.aZoneFiles[i]; // Found existing mapping
            // fprintf(stderr, "ZNS VFS DEBUG: Found existing mapping for WAL %s to Zone %s\n", zBaseName, zZoneFile);
            break;
        }
    }

    /* 2. If not found, find the first available free zone */
    if (!zZoneFile)
    {
        for (i = 0; i < zoneManager.nZones; i++)
        {
            if (zoneManager.aZoneState[i] == 0) /* Look for a Free zone */
            {
                // Attempt to allocate this zone
                zoneManager.aWalNames[i] = sqlite3_mprintf("%s", zBaseName); // Store WAL base name
                if (!zoneManager.aWalNames[i])
                {
                    fprintf(stderr, "ZNS VFS Error: Failed to allocate memory for WAL name mapping (%s).\n", zBaseName);
                    // Leave zone state as 0, break and return NULL below
                    break;
                }
                zoneManager.aZoneState[i] = 1;         /* Mark as Allocated */
                zZoneFile = zoneManager.aZoneFiles[i]; // Assign the zone file path
                // fprintf(stderr, "ZNS VFS DEBUG: Allocated new Zone %s (Index %d) for WAL %s\n", zZoneFile, i, zBaseName);
                break; /* Exit loop after finding and allocating a free zone */
            }
        }
        if (!zZoneFile && i == zoneManager.nZones) // Check if loop finished without finding free zone
        {
            fprintf(stderr, "ZNS VFS Warning: No free zone found for WAL %s (Checked %d zones).\n", zBaseName, zoneManager.nZones);
            // Fall through to return NULL
        }
        else if (!zZoneFile && zoneManager.aWalNames[i] == NULL)
        {
            // Name allocation failed inside loop
            // Zone state is still 0
            fprintf(stderr, "ZNS VFS Error: Allocation failed, could not assign zone for WAL %s.\n", zBaseName);
        }
    }

    sqlite3_mutex_leave(zoneManager.mutex);
    return zZoneFile; /* Returns pointer to path string in zoneManager.aZoneFiles or NULL */
}

/* --- VFS Initialization and Registration --- */

/*
** Initialize the ZNS VFS structure.
*/
static int znsVfsInit(sqlite3_vfs *pDefaultVfs)
{
    /* The VFS structure registered with SQLite */
    static sqlite3_vfs zns_vfs_struct;
    /* Contains the pointer to the real VFS */
    static zns_vfs zns_vfs_impl;

    /* Make sure the default VFS is valid */
    if (pDefaultVfs == NULL)
    {
        return SQLITE_ERROR;
    }

    /* Store the original VFS pointer if not already done */
    if (pOrigVfs == 0)
    {
        pOrigVfs = pDefaultVfs;
    }

    /* Initialize the wrapper VFS implementation data */
    // memset(&zns_vfs_impl, 0, sizeof(zns_vfs_impl)); // Only needs pRealVfs
    zns_vfs_impl.pRealVfs = pOrigVfs;

    /* Initialize the public VFS structure */
    memset(&zns_vfs_struct, 0, sizeof(sqlite3_vfs));
    zns_vfs_struct.iVersion = 3; /* VFS API version */
    /* Our file handle contains zns_file + space for the original VFS file handle */
    zns_vfs_struct.szOsFile = sizeof(zns_file) + pOrigVfs->szOsFile - sizeof(sqlite3_file);
    zns_vfs_struct.mxPathname = pOrigVfs->mxPathname;
    zns_vfs_struct.pNext = 0;
    zns_vfs_struct.zName = "zns";            /* Name of this VFS */
    zns_vfs_struct.pAppData = &zns_vfs_impl; /* Link to our implementation details */

    /* VFS methods */
    zns_vfs_struct.xOpen = znsOpen;
    zns_vfs_struct.xDelete = znsDelete;
    zns_vfs_struct.xAccess = znsAccess;
    zns_vfs_struct.xFullPathname = znsFullPathname;
    zns_vfs_struct.xDlOpen = znsDlOpen;
    zns_vfs_struct.xDlError = znsDlError;
    zns_vfs_struct.xDlSym = znsDlSym;
    zns_vfs_struct.xDlClose = znsDlClose;
    zns_vfs_struct.xRandomness = znsRandomness;
    zns_vfs_struct.xSleep = znsSleep;
    zns_vfs_struct.xCurrentTime = znsCurrentTime;
    zns_vfs_struct.xGetLastError = znsGetLastError;
    zns_vfs_struct.xCurrentTimeInt64 = znsCurrentTimeInt64;
    /* Optional system call methods if needed */
    // zns_vfs_struct.xSetSystemCall = znsSetSystemCall;
    // zns_vfs_struct.xGetSystemCall = znsGetSystemCall;
    // zns_vfs_struct.xNextSystemCall = znsNextSystemCall;

    /* Register the ZNS VFS. Make it NOT the default initially. */
    fprintf(stderr, "ZNS VFS INFO: Registering VFS 'zns'.\n");
    int rc = sqlite3_vfs_register(&zns_vfs_struct, 0);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS VFS Error: Failed to register VFS 'zns' (rc=%d).\n", rc);
    }
    return rc;
}

/*
** Entry point for the ZNS VFS extension.
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int
sqlite3_zns_init(
    sqlite3 *db,                     /* Unused */
    char **pzErrMsg,                 /* Error message out */
    const sqlite3_api_routines *pApi /* API routines */
)
{
    int rc = SQLITE_OK;
    sqlite3_vfs *pDefaultVfs;
    SQLITE_EXTENSION_INIT2(pApi);
    (void)db; // Mark db as unused

    fprintf(stderr, "ZNS VFS Extension: sqlite3_zns_init called.\n");

    /* Ensure the Zone Manager mutex is created early */
    if (!zoneManager.mutex)
    {
        zoneManager.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
        if (!zoneManager.mutex)
        {
            fprintf(stderr, "ZNS VFS Error: Failed to allocate Zone Manager mutex.\n");
            if (pzErrMsg)
                *pzErrMsg = sqlite3_mprintf("Failed to allocate mutex");
            return SQLITE_NOMEM;
        }
    }

    /* Find the default VFS to wrap */
    pDefaultVfs = sqlite3_vfs_find(0);
    if (pDefaultVfs == 0)
    {
        fprintf(stderr, "ZNS VFS Error: Cannot find default VFS during init.\n");
        if (pzErrMsg)
            *pzErrMsg = sqlite3_mprintf("Cannot find default VFS");
        rc = SQLITE_ERROR;
        goto init_failed;
    }
    /* Avoid re-registering if already registered */
    if (sqlite3_vfs_find("zns"))
    {
        fprintf(stderr, "ZNS VFS INFO: VFS 'zns' already registered.\n");
        /* Should we still try to init the zone manager? Yes, if path changed. */
    }
    else
    {
        /* Initialize and register the ZNS VFS structure */
        rc = znsVfsInit(pDefaultVfs);
        if (rc != SQLITE_OK)
        {
            fprintf(stderr, "ZNS VFS Error: Failed to initialize or register VFS (rc=%d).\n", rc);
            if (pzErrMsg)
                *pzErrMsg = sqlite3_mprintf("Failed to initialize ZNS VFS (rc=%d)", rc);
            goto init_failed;
        }
    }

    /* Initialize the Zone Manager if ZNS path is already set globally */
    /* This requires the external sqlite3WalGetZnsSsdPath/UseZnsSsd functions */
    const char *currentZnsPath = sqlite3WalGetZnsSsdPath();
    if (sqlite3WalUseZnsSsd() && currentZnsPath)
    {
        rc = znsZoneManagerInit(currentZnsPath);
        if (rc != SQLITE_OK)
        {
            fprintf(stderr, "ZNS VFS Error: Failed to initialize Zone Manager during extension init (rc=%d).\n", rc);
            if (pzErrMsg)
                *pzErrMsg = sqlite3_mprintf("Failed to initialize Zone Manager (rc=%d)", rc);
            /* Should we unregister the VFS here? Maybe not, allow manual init later */
            goto init_failed; // Treat failure to init manager here as fatal for extension load?
        }
    }
    else
    {
        fprintf(stderr, "ZNS VFS INFO: ZNS mode not enabled or path not set at extension init time. Call sqlite3_wal_use_zns() to enable.\n");
    }

    fprintf(stderr, "ZNS VFS Extension: Initialization complete (rc=%d).\n", rc);
    return rc;

init_failed:
    /* Clean up mutex if allocated and init failed */
    if (zoneManager.mutex && !zoneManager.zZnsPath)
    { // Only free if manager didn't fully init
        sqlite3_mutex_free(zoneManager.mutex);
        zoneManager.mutex = NULL;
    }
    return rc;
}

/*
** Public API function to enable/disable ZNS mode and set the path.
** This function MUST be called AFTER sqlite3_initialize() or loading the extension,
** and BEFORE opening any database connections that should use ZNS WAL.
*/
SQLITE_API int sqlite3_wal_use_zns(const char *znsPath)
{
    int rc = SQLITE_OK;

    fprintf(stderr, "ZNS VFS API: sqlite3_wal_use_zns called with path: %s\n", znsPath ? znsPath : "<null>");

    /* Ensure the VFS is registered (it might not be default) */
    if (!sqlite3_vfs_find("zns"))
    {
        fprintf(stderr, "ZNS VFS Error: 'zns' VFS not registered. Load the extension first.\n");
        return SQLITE_ERROR; // Or SQLITE_MISUSE?
    }
    /* Ensure the mutex is created if not already (might happen if extension loaded but no path set) */
    if (!zoneManager.mutex)
    {
        zoneManager.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
        if (!zoneManager.mutex)
        {
            fprintf(stderr, "ZNS VFS Error: Failed to allocate Zone Manager mutex in API call.\n");
            return SQLITE_NOMEM;
        }
    }

    if (znsPath == 0 || znsPath[0] == 0)
    {
        fprintf(stderr, "ZNS VFS API: Disabling ZNS mode.\n");
        sqlite3WalEnableZnsSsd(0);  // Set global flag OFF
        sqlite3WalSetZnsSsdPath(0); // Clear global path
        znsZoneManagerDestroy();    // Clean up the manager
        return SQLITE_OK;
    }

    /* Check if the provided path is a valid directory */
    struct stat st;
    if (stat(znsPath, &st) != 0)
    {
        fprintf(stderr, "ZNS VFS Error: Cannot stat ZNS path '%s': %s\n", znsPath, strerror(errno));
        return SQLITE_CANTOPEN; // Path does not exist or other error
    }
    if (!S_ISDIR(st.st_mode))
    {
        fprintf(stderr, "ZNS VFS Error: ZNS path '%s' is not a directory.\n", znsPath);
        return SQLITE_MISUSE; // Incorrect usage
    }

    /* Set the global path string (defined externally) */
    sqlite3WalSetZnsSsdPath(znsPath);
    /* Enable the global flag (defined externally) */
    sqlite3WalEnableZnsSsd(1);

    /* Initialize (or re-initialize if path changed) the Zone Manager */
    rc = znsZoneManagerInit(znsPath);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS VFS Error: Failed to initialize Zone Manager for path '%s' (rc=%d).\n", znsPath, rc);
        /* Disable ZNS mode again if manager init failed */
        sqlite3WalEnableZnsSsd(0);
        sqlite3WalSetZnsSsdPath(0);
        return rc;
    }

    fprintf(stderr, "ZNS VFS API: Enabled ZNS mode with path: %s\n", znsPath);
    return SQLITE_OK;
}