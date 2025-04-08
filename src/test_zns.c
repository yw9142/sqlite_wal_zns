#include <stdio.h>
#include <sqlite3.h>

// ZNS VFS 확장 모듈 로드를 위한 함수 선언
int sqlite3_zns_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
int sqlite3_wal_use_zns(const char *zPath);

int main()
{
    sqlite3 *db;
    char *errmsg = 0;
    int rc;

    // 데이터베이스 연결
    rc = sqlite3_open("test_zns.db", &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "데이터베이스 열기 실패: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // ZNS 확장 모듈 로드 (os_zns.c 내의 함수를 사용 가능하게 함)
    sqlite3_enable_load_extension(db, 1);
    rc = sqlite3_load_extension(db, "./libsqlite3_zns", "sqlite3_zns_init", &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS 확장 모듈 로드 실패: %s\n", errmsg);
        sqlite3_free(errmsg);
    }

    // ZNS SSD 경로 설정 및 활성화 (통합 함수 사용)
    rc = sqlite3_wal_use_zns("/mnt");
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS 설정 실패: %d\n", rc);
    }

    printf("ZNS SSD 설정 완료\n");

    // ZNS VFS 명시적 사용
    rc = sqlite3_exec(db, "PRAGMA vfs='zns';", 0, 0, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS VFS 설정 실패: %s\n", errmsg);
        sqlite3_free(errmsg);
    }

    // WAL 모드 활성화
    rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", 0, 0, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "WAL 모드 설정 실패: %s\n", errmsg);
        sqlite3_free(errmsg);
    }

    // 테이블 생성
    rc = sqlite3_exec(db,
                      "CREATE TABLE IF NOT EXISTS test(id INTEGER PRIMARY KEY, data TEXT);",
                      0, 0, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL 에러: %s\n", errmsg);
        sqlite3_free(errmsg);
    }

    // 데이터 삽입
    for (int i = 1; i <= 1000; i++)
    {
        char sql[100];
        sprintf(sql, "INSERT INTO test VALUES(%d, 'ZNS WAL 테스트 데이터 #%d');", i, i);
        rc = sqlite3_exec(db, sql, 0, 0, &errmsg);
        if (rc != SQLITE_OK)
        {
            fprintf(stderr, "SQL 에러: %s\n", errmsg);
            sqlite3_free(errmsg);
            break;
        }

        // 중간에 체크포인트 추가 (WAL 파일이 생성되고 동작하는지 확인)
        if (i % 100 == 0)
        {
            printf("%d개 레코드 삽입 완료, 체크포인트 실행\n", i);
            sqlite3_exec(db, "PRAGMA wal_checkpoint;", 0, 0, 0);
        }
    }

    // WAL 관련 정보 출력
    printf("WAL 파일 정보 확인:\n");
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "PRAGMA wal_status;", -1, &stmt, 0);
    if (rc == SQLITE_OK)
    {
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            printf("  - %s: %s\n",
                   sqlite3_column_text(stmt, 0),
                   sqlite3_column_text(stmt, 1));
        }
        sqlite3_finalize(stmt);
    }

    printf("테스트 완료!\n");
    sqlite3_close(db);
    return 0;
}