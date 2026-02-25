#!/bin/bash
set -e

# 환경 변수 확인 (디버깅용, 실제 운영 시에는 비밀번호 echo 금지)
echo "Creating database user: $SIMULATOR_USER"

# psql 명령어를 사용하여 SQL 실행 (환경 변수 사용 가능)
# <<-EOSQL ... EOSQL 블록 안에서는 $변수명이 실제 값으로 치환됩니다.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- 유저 생성 (환경 변수 사용)
    CREATE USER $SIMULATOR_USER WITH PASSWORD '$SIMULATOR_PASSWORD';

    -- 권한 부여
    GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO $SIMULATOR_USER;
    
    -- 스키마 권한 부여
    \connect $POSTGRES_DB
    GRANT ALL ON SCHEMA public TO $SIMULATOR_USER;
EOSQL

echo "User creation completed."