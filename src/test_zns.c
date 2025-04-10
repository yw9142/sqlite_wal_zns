#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <string.h> // For strcmp

// --- 사용자 설정 값 ---
// !!! 이 경로들을 실제 환경에 맞게 수정하세요 !!!
const char *ZONEFS_PATH = "/mnt/zonefs";    // 실제 ZoneFS 마운트 경로
const char *DB_DIR_PATH = "/tmp";           // 원본 DB 파일이 저장될 일반 파일 시스템 경로
const char *DB_FILENAME = "test_zns_db.db"; // 생성될 DB 파일 이름
// --------------------

// --- SQLite ZNS 관련 함수 선언 ---
// 실제로는 sqlite3.h 또는 별도 헤더에 있어야 하지만, 테스트를 위해 여기에 선언합니다.
// 실제 SQLite 빌드 환경에서는 필요 없을 수 있습니다.
extern void sqlite3WalEnableZnsSsd(int enable);
extern void sqlite3WalSetZnsSsdPath(const char *zPath);
// --------------------------------

// 콜백 함수 (PRAGMA 결과 확인용)
static int callback(void *data, int argc, char **argv, char **azColName)
{
    // journal_mode 확인용
    if (argc > 0 && argv[0])
    {
        printf("  > %s = %s\n", azColName[0], argv[0]);
        // 결과를 data 포인터에 저장 (문자열 비교용)
        if (data)
        {
            strncpy((char *)data, argv[0], 64 - 1);
            ((char *)data)[64 - 1] = '\0';
        }
    }
    return 0;
}

int main()
{
    sqlite3 *db = NULL;
    char *zErrMsg = 0;
    int rc;
    char db_path[256];
    char journal_mode_result[64] = {0}; // PRAGMA 결과 저장

    snprintf(db_path, sizeof(db_path), "%s/%s", DB_DIR_PATH, DB_FILENAME);

    printf("ZNS WAL 저장 테스트 시작...\n");
    printf("ZoneFS 경로: %s\n", ZONEFS_PATH);
    printf("DB 파일 경로: %s\n", db_path);

    // 1. ZNS 모드 활성화 및 경로 설정 (!!! 중요 !!!)
    printf("\n[단계 1] ZNS WAL 모드 활성화 및 경로 설정 중...\n");
    sqlite3WalEnableZnsSsd(1);            // ZNS 모드 켜기
    sqlite3WalSetZnsSsdPath(ZONEFS_PATH); // ZoneFS 경로 설정
    printf("  > ZNS WAL 모드 활성화됨 (useZnsSsd=1)\n");
    printf("  > ZNS WAL 경로 설정됨: %s\n", ZONEFS_PATH);

    // 2. 데이터베이스 열기 (일반 파일 시스템 경로에 생성)
    printf("\n[단계 2] 데이터베이스 파일 열기 시도 (%s)...\n", db_path);
    rc = sqlite3_open(db_path, &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "  오류: 데이터베이스를 열 수 없음: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db); // 실패 시에도 close 호출 (NULL이어도 안전)
        return 1;
    }
    printf("  > 데이터베이스 열기 성공.\n");

    // 3. 저널 모드를 WAL로 설정
    printf("\n[단계 3] 저널 모드를 WAL로 설정 시도...\n");
    rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", 0, 0, &zErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "  오류: WAL 모드 설정 실패: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        sqlite3_close(db);
        return 1;
    }
    printf("  > WAL 모드 설정 명령 실행됨.\n");

    // 3.1 저널 모드 확인
    printf("  > 현재 저널 모드 확인 중...\n");
    rc = sqlite3_exec(db, "PRAGMA journal_mode;", callback, journal_mode_result, &zErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "  오류: 저널 모드 확인 실패: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        sqlite3_close(db);
        return 1;
    }
    if (strcmp(journal_mode_result, "wal") != 0)
    {
        fprintf(stderr, "  오류: 저널 모드가 'wal'로 설정되지 않았습니다! (%s)\n", journal_mode_result);
        sqlite3_close(db);
        return 1;
    }
    printf("  > 저널 모드가 'wal'로 성공적으로 설정됨 확인.\n");

    // 4. 데이터베이스에 쓰기 작업 수행 (WAL 파일 생성 유도)
    printf("\n[단계 4] 데이터베이스 쓰기 작업 수행 (WAL 파일 생성 유도)...\n");
    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS test_tbl (id INTEGER PRIMARY KEY, data TEXT);"
                          "INSERT INTO test_tbl (data) VALUES ('Hello ZNS WAL!');",
                      0, 0, &zErrMsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "  오류: 쓰기 작업 실패: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        sqlite3_close(db);
        return 1;
    }
    printf("  > 쓰기 작업 성공.\n");

    // 5. 데이터베이스 닫기
    printf("\n[단계 5] 데이터베이스 닫기...\n");
    rc = sqlite3_close(db);
    if (rc != SQLITE_OK)
    {
        // WAL 모드에서는 연결이 남아있으면 SQLITE_BUSY 오류가 날 수 있음
        fprintf(stderr, "  경고 또는 오류: 데이터베이스 닫기 중 문제 발생: %s\n", sqlite3_errmsg(db));
        // 이 테스트에서는 무시하고 진행
    }
    printf("  > 데이터베이스 닫기 완료 (또는 시도됨).\n");

    // 6. 결과 확인 안내
    printf("\n테스트 완료.\n");
    printf("이제 다음 경로에 WAL 파일이 생성되었는지 확인하세요:\n");
    printf("  %s/%s-wal\n", ZONEFS_PATH, DB_FILENAME);
    printf("\n예시 명령어:\n");
    printf("  ls -l %s/%s-wal\n", ZONEFS_PATH, DB_FILENAME);

    return 0;
}