from typing import List
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False
    )

    # Node Identity & Ports
    NODE_ID: str = Field(default="node-1", description="Unique Raft Node Identifier")
    HTTP_PORT: int = Field(default=8001, description="FastAPI HTTP Service Port")
    GRPC_PORT: int = Field(default=50051, description="Raft Consensus gRPC Peer Port")
    
    # Comma-separated peer list, e.g. "node-2:50052,node-3:50053"
    RAFT_PEERS: str = Field(
        default="node-2:50052,node-3:50053", 
        description="Comma-separated list of host:port peer endpoints"
    )

    # Raft Election Timeout Range & Heartbeat Interval (in milliseconds)
    ELECTION_TIMEOUT_MIN_MS: int = Field(default=1000, description="Minimum Election Timeout (ms)")
    ELECTION_TIMEOUT_MAX_MS: int = Field(default=2000, description="Maximum Election Timeout (ms)")
    HEARTBEAT_INTERVAL_MS: int = Field(default=150, description="Leader Heartbeat Interval (ms)")

    # Database & Storage Configuration
    DATABASE_URL: str = Field(
        default="sqlite:///./dropbox.db",
        description="SQLAlchemy Database Connection String (PostgreSQL or SQLite)"
    )
    MINIO_ENDPOINT: str = Field(default="localhost:9000", description="MinIO S3 Endpoint")
    MINIO_ACCESS_KEY: str = Field(default="minioadmin", description="MinIO Access Key")
    MINIO_SECRET_KEY: str = Field(default="minioadmin", description="MinIO Secret Key")
    MINIO_SECURE: bool = Field(default=False, description="Use TLS for MinIO connections")
    MINIO_BUCKET: str = Field(default="dropbox-files", description="Default MinIO Bucket")
    MINIO_PEER_ENDPOINTS: str = Field(
        default="node-2=localhost:9002,node-3=localhost:9003",
        description="Comma-separated list of peer_id=endpoint MinIO peer endpoints"
    )

    # Security & Authentication
    JWT_SECRET_KEY: str = Field(
        default="dev-secret-key-node-1-change-me-super-secure-32bytes",
        description="Secret key for JWT token signing (32+ bytes)"
    )
    JWT_ALGORITHM: str = Field(default="HS256", description="JWT Algorithm")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=60, description="Token Expiration Minutes")
    
    # Cookie Settings for HttpOnly Auth
    COOKIE_NAME: str = Field(default="access_token", description="HttpOnly Auth Cookie Name")
    COOKIE_SECURE: bool = Field(default=False, description="HttpOnly Cookie Secure Flag")
    COOKIE_SAMESITE: str = Field(default="lax", description="HttpOnly Cookie SameSite Flag")

    # Argon2id Password Hashing Configuration
    ARGON2_MEMORY_COST: int = Field(default=65536, description="Argon2id Memory Cost in KiB")
    ARGON2_TIME_COST: int = Field(default=3, description="Argon2id Time Cost (Iterations)")
    ARGON2_PARALLELISM: int = Field(default=4, description="Argon2id Parallelism Threads")

    # File Upload Security Limits
    MAX_UPLOAD_SIZE_BYTES: int = Field(default=104857600, description="Maximum Upload Size (100MB)")

    # Snapshot & Compaction Threshold
    SNAPSHOT_THRESHOLD: int = Field(default=10, description="Raft Snapshot Trigger Threshold (log entries)")

    # Operational Parameters
    ENVIRONMENT: str = Field(default="development", description="Execution Environment")
    PHASE: str = Field(default="phase-7", description="Current Implementation Phase")
    LOG_LEVEL: str = Field(default="INFO", description="Logging Level")

    @model_validator(mode="after")
    def validate_production_environment(self) -> "Settings":
        if self.ENVIRONMENT.lower() in ("production", "prod"):
            insecure_jwt_patterns = ["change-this", "change-me", "dev-secret", "super-secret", "default"]
            if any(pattern in self.JWT_SECRET_KEY.lower() for pattern in insecure_jwt_patterns) or len(self.JWT_SECRET_KEY) < 32:
                raise ValueError(
                    "Insecure JWT_SECRET_KEY detected in production environment! "
                    "Must provide a strong, unique 32+ character key."
                )
            if self.MINIO_ACCESS_KEY.lower() == "minioadmin" or self.MINIO_SECRET_KEY.lower() == "minioadmin":
                raise ValueError(
                    "Default MinIO credentials ('minioadmin') are prohibited in production environment! "
                    "Configure secure credentials in .env."
                )
        return self

    @property
    def peer_list(self) -> List[str]:
        if not self.RAFT_PEERS:
            return []
        return [peer.strip() for peer in self.RAFT_PEERS.split(",") if peer.strip()]


settings = Settings()
