import pytest
from app.db.base import Base
from app.db.session import engine


@pytest.fixture(autouse=True)
def setup_test_database():
    """Ensure database tables are fresh for every test execution."""
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
