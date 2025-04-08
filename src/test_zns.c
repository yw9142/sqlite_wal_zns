#include <stdio.h>
#include <sqlite3.h>

int main()
{
    sqlite3 *db;
    char *errmsg = 0;
    int rc;

    // 데이터베이스 연결
    rc = sqlite3_open("test_zns.db", &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "데이터베이스 열기 실패: %s\\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // ZNS SSD 경로 설정 (zonefs가 마운트된 경로)
    sqlite3_exec(db, "SELECT sqlite3_wal_set_zns_path('/mnt/zonefs');", 0, 0, 0);

    // ZNS SSD 기능 활성화
    sqlite3_exec(db, "SELECT sqlite3_wal_enable_zns(1);", 0, 0, 0);

    // WAL 모드 활성화
    rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", 0, 0, 0);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "WAL 모드 설정 실패: %s\\n", sqlite3_errmsg(db));
    }

    // 테이블 생성
    rc = sqlite3_exec(db,
                      "CREATE TABLE IF NOT EXISTS test(id INTEGER PRIMARY KEY, data TEXT);",
                      0, 0, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL 에러: %s\\n", errmsg);
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
            fprintf(stderr, "SQL 에러: %s\\n", errmsg);
            sqlite3_free(errmsg);
            break;
        }
    }

    printf("테스트 완료!\\n");
    sqlite3_close(db);
    return 0;
}
