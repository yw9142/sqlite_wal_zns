#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sqlite3.h>

// ZNS VFS 확장 모듈 로드를 위한 함수 선언
int sqlite3_zns_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
int sqlite3_wal_use_zns(const char *zPath);

// 디렉토리 내 파일 탐색 함수
void listFiles(const char *path, const char *pattern)
{
    DIR *dir;
    struct dirent *entry;

    if (!(dir = opendir(path)))
    {
        printf("디렉토리 열기 실패: %s\n", path);
        return;
    }

    printf("%s 디렉토리의 파일 목록:\n", path);
    while ((entry = readdir(dir)) != NULL)
    {
        if (entry->d_type == DT_REG)
        { // 일반 파일만
            if (pattern == NULL || strstr(entry->d_name, pattern) != NULL)
            {
                printf("  - %s\n", entry->d_name);
            }
        }
    }
    closedir(dir);
}

int callback(void *NotUsed, int argc, char **argv, char **azColName)
{
    for (int i = 0; i < argc; i++)
    {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

int main(int argc, char *argv[])
{
    sqlite3 *db;
    char *errmsg = 0;
    int rc;
    char *zns_mount_path;

    // 명령행 인수로 ZNS 마운트 경로를 받음
    if (argc < 2)
    {
        printf("사용법: %s <ZNS_마운트_경로>\n", argv[0]);
        printf("예: %s /mnt\n", argv[0]);
        return 1;
    }

    zns_mount_path = argv[1];
    printf("ZNS 마운트 경로: %s\n", zns_mount_path);

    // 현재 디렉토리 파일 목록 확인 (데이터베이스 생성 전)
    printf("테스트 시작 전:\n");
    listFiles(".", "test_zns_");
    listFiles(zns_mount_path, NULL);

    // 데이터베이스 연결
    printf("\n1. 데이터베이스 연결 중...\n");
    rc = sqlite3_open("test_zns_verify.db", &db);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "데이터베이스 열기 실패: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // ZNS 확장 모듈 로드
    printf("2. ZNS 확장 모듈 로드 중...\n");
    sqlite3_enable_load_extension(db, 1);
    rc = sqlite3_load_extension(db, "./libsqlite3_zns", "sqlite3_zns_init", &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS 확장 모듈 로드 실패: %s\n", errmsg);
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return 1;
    }

    // ZNS SSD 경로 설정 (zonefs 마운트 포인트)
    printf("3. ZNS SSD 경로 설정: %s\n", zns_mount_path);
    rc = sqlite3_wal_use_zns(zns_mount_path);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS 설정 실패: %d\n", rc);
        sqlite3_close(db);
        return 1;
    }

    // ZNS VFS 사용 설정
    printf("4. ZNS VFS 활성화 중...\n");
    rc = sqlite3_exec(db, "PRAGMA vfs='zns';", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "ZNS VFS 설정 실패: %s\n", errmsg);
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return 1;
    }

    // WAL 모드 활성화
    printf("5. WAL 모드 활성화 중...\n");
    rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", callback, NULL, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "WAL 모드 설정 실패: %s\n", errmsg);
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return 1;
    }

    // WAL 모드 설정 확인
    printf("\nWAL 모드 설정 확인 중...\n");
    listFiles(".", "test_zns_verify*");
    listFiles(zns_mount_path, NULL);

    // 테이블 생성
    printf("\n6. 테이블 생성 중...\n");
    rc = sqlite3_exec(db,
                      "CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT);",
                      NULL, NULL, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "테이블 생성 실패: %s\n", errmsg);
        sqlite3_free(errmsg);
        sqlite3_close(db);
        return 1;
    }

    // 데이터 삽입
    printf("7. 데이터 삽입 중...\n");
    for (int i = 1; i <= 100; i++)
    {
        char sql[100];
        sprintf(sql, "INSERT INTO test VALUES(%d, 'ZNS WAL 테스트 데이터 #%d');", i, i);
        rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
        if (rc != SQLITE_OK)
        {
            fprintf(stderr, "데이터 삽입 실패: %s\n", errmsg);
            sqlite3_free(errmsg);
            break;
        }

        // 10개 레코드마다 체크포인트 수행
        if (i % 10 == 0)
        {
            printf("  %d개 레코드 삽입 완료, 체크포인트 실행\n", i);
            sqlite3_exec(db, "PRAGMA wal_checkpoint;", NULL, NULL, NULL);

            // 현재 상태 확인
            printf("\n현재 상태 확인:\n");
            listFiles(".", "test_zns_verify*");
            listFiles(zns_mount_path, NULL);
        }
    }

    // WAL 관련 정보 출력
    printf("\n8. WAL 파일 정보 확인 중...\n");
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "PRAGMA wal_status;", -1, &stmt, NULL);
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

    // 최종 파일 확인
    printf("\n9. 최종 파일 상태 확인:\n");
    listFiles(".", "test_zns_verify*");
    listFiles(zns_mount_path, NULL);

    // 데이터베이스 연결 종료
    printf("\n10. 데이터베이스 연결 종료 중...\n");
    sqlite3_close(db);

    printf("\n테스트 완료!\n");
    return 0;
}