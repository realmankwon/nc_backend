import dataset
import os

databaseConnector = os.getenv('DB_CONNECTOR')

# dataset.connect를 사용하여 데이터베이스 연결 설정
db = dataset.connect(
    databaseConnector,
    engine_kwargs={
        'pool_recycle': 3600      # 재활용 시간 (초)
    }
)

def get_db_connection():
    """데이터베이스 연결을 반환하는 공용 함수."""
    try:
        return db
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
