import base64
from datetime import datetime
import gc
import json
import logging
import random
import time
from typing import Any, Dict, List, Optional

import aiohttp
import asyncpg
from cachetools import TTLCache
import pytz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection pool
_pg_pool = None
_postgres_uri = None

# Caches
_character_cache = TTLCache(maxsize=500, ttl=1800)  # 30 minutes
_drop_settings_cache = TTLCache(maxsize=5, ttl=900)  # 15 minutes
_user_stats_cache = TTLCache(maxsize=100, ttl=600)  # 10 minutes
_leaderboard_cache = TTLCache(maxsize=3, ttl=180)  # 3 minutes
_chat_settings_cache = TTLCache(maxsize=25, ttl=900)  # 15 minutes

# Performance tracking
_performance_stats = {
    'total_queries': 0,
    'cache_hits': 0,
    'cache_misses': 0,
    'last_cleanup': time.time(),
    'memory_usage': [],
    'connection_errors': 0,
    'slow_queries': 0
}

# Define rarity system (same as MongoDB module)
RARITIES = {
    "Common": 1,
    "Medium": 2,
    "Rare": 3,
    "Legendary": 4,
    "Exclusive": 5,
    "Elite": 6,
    "Limited Edition": 7,
    "Ultimate": 8,
    "Supreme": 9,
    "Zenith": 10,
    "Ethereal": 11,
    "Mythic": 12,
    "Premium": 13
}

# Define emoji mappings for rarities (same as MongoDB module)
RARITY_EMOJIS = {
    "Common": "⚪️",
    "Medium": "🟢",
    "Rare": "🟠",
    "Legendary": "🟡",
    "Exclusive": "🫧",
    "Elite": "💎",
    "Limited Edition": "🔮",
    "Ultimate": "🔱",
    "Supreme": "👑",
    "Zenith": "💫",
    "Ethereal": "❄️",
    "Mythic": "🔴",
    "Premium": "🧿"
}

def get_rarity_display(rarity: str) -> str:
    """Get the display format for a rarity (emoji + name)"""
    emoji = RARITY_EMOJIS.get(rarity, "❓")
    return f"{emoji} {rarity}"

def get_rarity_emoji(rarity: str) -> str:
    """Get just the emoji for a rarity"""
    return RARITY_EMOJIS.get(rarity, "❓")

# Update the migration mapping (same as MongoDB module)
OLD_TO_NEW_RARITIES = {
    "⚪️ Common": "Common",
    "🟢 Medium": "Medium",
    "🟠 Rare": "Rare",
    "🟡 Legendary": "Legendary",
    "🫧 Exclusive": "Exclusive",
    "💎 Elite": "Elite",
    "🔮 Limited Edition": "Limited Edition",
    "🔱 Ultimate": "Ultimate",
    "👑 Supreme": "Supreme",
    "💫 Zenith": "Zenith",
    "❄️ Ethereal": "Ethereal",
    "🔴 Mythic": "Mythic",
    "🧿 Premium": "Premium"
}

def get_performance_stats():
    """Get performance statistics"""
    return _performance_stats.copy()

def clear_all_caches():
    """Clear all caches"""
    _character_cache.clear()
    _drop_settings_cache.clear()
    _user_stats_cache.clear()
    _leaderboard_cache.clear()
    _chat_settings_cache.clear()

def get_postgres_pool():
    """Get the PostgreSQL connection pool"""
    global _pg_pool
    if _pg_pool is None:
        raise RuntimeError("PostgreSQL pool not initialized. Call init_database() first.")
    return _pg_pool

async def init_database(postgres_uri: str):
    """Initialize PostgreSQL connection pool"""
    global _pg_pool, _postgres_uri, _db_instance
    
    if _pg_pool is None:
        try:
            _postgres_uri = postgres_uri
            _pg_pool = await asyncpg.create_pool(
                postgres_uri,
                min_size=5,
                max_size=20,
                command_timeout=30,
                server_settings={
                    'jit': 'off',  # Disable JIT for better performance
                    'statement_timeout': '30000',  # 30 seconds
                    'idle_in_transaction_session_timeout': '60000'  # 1 minute
                }
            )
            
            # Test connection
            async with _pg_pool.acquire() as conn:
                await conn.execute('SELECT 1')
            
            # Create database instance
            _db_instance = PostgresDatabase()
            pass  # Connection pool initialized
            # Run migrations for missing columns
            await run_column_migrations()
            
            # Ensure redeem_codes table is properly set up
            try:
                db_instance = PostgresDatabase()
                await db_instance.fix_redeem_codes_table()
            except Exception as e:
                print(f"⚠️ Could not fix redeem_codes table during init: {e}")
            
        except Exception as e:
            pass  # Error initializing pool
            raise
            async with _pg_pool.acquire() as conn:
                await conn.execute('''
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS bank BIGINT DEFAULT 0;
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS ban_reason TEXT;
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS ban_until TIMESTAMP;
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS supreme_store_offer JSONB DEFAULT '{}';
                ''')

async def run_column_migrations():
    """Ensure all required columns exist in tables."""
    global _pg_pool
    async with _pg_pool.acquire() as conn:
        # Users table columns
        await conn.execute('''
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS bank BIGINT DEFAULT 0;
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS ban_reason TEXT;
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS ban_until TIMESTAMP;
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS groups BIGINT[] DEFAULT ARRAY[]::BIGINT[];
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS supreme_store_offer JSONB DEFAULT '{}';
        ''')
        # Characters table columns
        await conn.execute('''
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='characters' AND column_name='added_by'
                ) THEN
                    ALTER TABLE characters ADD COLUMN added_by BIGINT;
                END IF;
            END$$;
        ''')
        # Ensure character_id is auto-incrementing and unique
        await conn.execute('''
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='characters' AND column_name='character_id'
                ) THEN
                    ALTER TABLE characters ADD COLUMN character_id SERIAL PRIMARY KEY;
                ELSE
                    BEGIN
                        -- If not identity, alter to identity (PostgreSQL 12+)
                        BEGIN
                            EXECUTE 'ALTER TABLE characters ALTER COLUMN character_id ADD GENERATED ALWAYS AS IDENTITY';
                        EXCEPTION WHEN others THEN NULL; -- Ignore if already identity
                        END;
                        -- Remove duplicates if any
                        DELETE FROM characters a USING characters b
                        WHERE a.ctid < b.ctid AND a.character_id = b.character_id;
                    END;
                END IF;
            END$$;
        ''')
        # Redeem codes table
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                type TEXT, -- 'character' (NULL), 'token', or 'shard'
                character_id INTEGER,
                token_amount INTEGER,
                shard_amount INTEGER,
                created_by BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                max_claims INTEGER NOT NULL DEFAULT 1,
                claims INTEGER NOT NULL DEFAULT 0,
                claimed_by BIGINT[] DEFAULT ARRAY[]::BIGINT[]
            );
        ''')
        
        # Ensure all required columns exist (for existing tables)
        await conn.execute('''
            DO $$
            BEGIN
                -- Add shard_amount if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='shard_amount'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN shard_amount INTEGER;
                END IF;
                
                -- Add token_amount if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='token_amount'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN token_amount INTEGER;
                END IF;
                
                -- Add type if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='type'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN type TEXT;
                END IF;
                
                -- Add character_id if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='character_id'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN character_id INTEGER;
                END IF;
                
                -- Add created_by if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='created_by'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN created_by BIGINT;
                END IF;
                
                -- Add created_at if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='created_at'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                END IF;
                
                -- Add max_claims if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='max_claims'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN max_claims INTEGER NOT NULL DEFAULT 1;
                END IF;
                
                -- Add claims if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='claims'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN claims INTEGER NOT NULL DEFAULT 0;
                END IF;
                
                -- Add claimed_by if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='redeem_codes' AND column_name='claimed_by'
                ) THEN
                    ALTER TABLE redeem_codes ADD COLUMN claimed_by BIGINT[] DEFAULT ARRAY[]::BIGINT[];
                END IF;
            END$$;
        ''')

class PostgresDatabase:
    async def reset_character_from_collections(self, character_id: int):
        """Remove character from all users' collections but keep in database."""
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE users SET characters = array_remove(characters, $1)", character_id)
        return True
    async def delete_character(self, character_id: int):
        """Delete character from database and remove from all user collections."""
        async with self.pool.acquire() as conn:
            # Remove character from characters table
            await conn.execute("DELETE FROM characters WHERE character_id = $1", character_id)
            # Remove character from all users' characters arrays
            await conn.execute("UPDATE users SET characters = array_remove(characters, $1)", character_id)
        # Invalidate character cache
        if character_id in _character_cache:
            del _character_cache[character_id]
        return True
    async def edit_character(self, character_id: int, update_data: dict):
        """Edit character fields by character_id."""
        if not update_data:
            return False
        set_clauses = []
        params = []
        idx = 1
        for key, value in update_data.items():
            set_clauses.append(f"{key} = ${idx}")
            params.append(value)
            idx += 1
        sql = f"UPDATE characters SET {', '.join(set_clauses)} WHERE character_id = ${idx}"
        params.append(character_id)
        async with self.pool.acquire() as conn:
            await conn.execute(sql, *params)
        # Invalidate character cache so updates are reflected
        if character_id in _character_cache:
            del _character_cache[character_id]
        return True
    async def add_character(self, character_data: dict):
        """Add a new character to the database and return its ID."""
        async with self.pool.acquire() as conn:
            # Remove explicit character_id if present
            data = dict(character_data)
            result = await conn.fetchrow(
                """
                INSERT INTO characters (name, rarity, file_id, img_url, is_video, added_by, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
                RETURNING character_id
                """,
                data.get("name"),
                data.get("rarity"),
                data.get("file_id"),
                data.get("img_url"),
                data.get("is_video", False),
                data.get("added_by")
            )
            return result["character_id"] if result else None
    async def add_user_to_group(self, user_id, group_id):
        """Add a user to a group by updating their groups array."""
        try:
            async with self.pool.acquire() as conn:
                # First ensure the groups column exists
                column_check = await conn.fetchrow("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'users' AND column_name = 'groups'
                """)
                
                if not column_check:
                    # Add the groups column if it doesn't exist
                    await conn.execute("""
                        ALTER TABLE users ADD COLUMN groups BIGINT[] DEFAULT ARRAY[]::BIGINT[];
                    """)
                
                # Add group_id to user's groups array if not already present
                await conn.execute("""
                    UPDATE users 
                    SET groups = CASE 
                        WHEN groups IS NULL THEN ARRAY[$2::bigint] 
                        WHEN NOT ($2 = ANY(groups)) THEN array_append(groups, $2::bigint)
                        ELSE groups
                    END
                    WHERE user_id = $1
                """, user_id, group_id)
                
        except Exception as e:
            logger.error(f"Error adding user {user_id} to group {group_id}: {e}")
    async def get_daily_drops(self, rarity):
        """Stub for get_daily_drops to prevent AttributeError. Returns 0."""
        return 0
    async def increment_daily_drops(self, user_id: int):
        """No-op stub for compatibility. Implement if needed."""
        pass
    async def ensure_collection_history_column(self):
        """Ensure the 'collection_history' column exists in the users table (JSONB, nullable)."""
        try:
            # Check if column exists
            check_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='users' AND column_name='collection_history'
            """
            async with self.pool.acquire() as conn:
                result = await conn.fetchval(check_query)
                if not result:
                    # Add the column
                    await conn.execute("""
                    ALTER TABLE users ADD COLUMN collection_history JSONB;
                    """)
                    # Create optimized indexes for JSONB queries
                    await self._create_collection_history_indexes(conn)
                    logger.info("Added collection_history column and indexes")
        except Exception as e:
            logger.error(f"Error ensuring collection_history column: {e}")

    async def _create_collection_history_indexes(self, conn):
        """Create optimized indexes for collection_history JSONB queries."""
        try:
            # GIN index for efficient JSONB operations
            await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_collection_history_gin 
            ON users USING GIN (collection_history);
            """)
            
            # BTREE index for source field lookups (text fields work better with BTREE)
            await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_collection_history_source 
            ON users ((collection_history->>'source'));
            """)
            
            # BTREE index for collected_at field lookups
            await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_collection_history_collected_at 
            ON users ((collection_history->>'collected_at'));
            """)
            
            # Composite index for source + collected_at queries
            await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_collection_history_source_collected_at 
            ON users ((collection_history->>'source'), (collection_history->>'collected_at'));
            """)
            
            logger.info("Created collection_history indexes")
        except Exception as e:
            logger.error(f"Error creating collection_history indexes: {e}")
    async def ensure_active_action_column(self):
        """Ensure the 'active_action' column exists in the users table (JSONB, nullable)."""
        query = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='users' AND column_name='active_action'
            ) THEN
                ALTER TABLE users ADD COLUMN active_action JSONB;
            END IF;
        END$$;
        """
        try:
            await self.pool.execute(query)
        except Exception as e:
            pass  # Error handling, optionally print or raise
            raise

    async def update_user_atomic(self, user_id, new_characters, wallet_delta, sold_entries):
        # Ensure collection_history column exists before update
        await self.ensure_collection_history_column()
        """Atomically update characters, wallet, and append to collection_history for a user."""
        query = """
        UPDATE users
        SET characters = $1,
            wallet = wallet + $2,
            collection_history = 
                CASE 
                    WHEN collection_history IS NULL OR jsonb_typeof(collection_history) != 'array' THEN 
                        $3::jsonb
                    ELSE 
                        collection_history || $3::jsonb
                END
        WHERE user_id = $4
        """
        import json
        def convert(obj):
            if isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(i) for i in obj]
            elif hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                return obj
        safe_entries = convert(sold_entries)
        try:
            await self.pool.execute(query, new_characters, wallet_delta, json.dumps(safe_entries), user_id)
        except Exception as e:
            pass  # Error handling, optionally print or raise
            raise
    async def set_favorite_character(self, user_id, character_id):
        """Set the user's favorite character by updating the favorite_character field."""
        query = """
            UPDATE users
            SET favorite_character = $1
            WHERE user_id = $2
        """
        try:
            await self.pool.execute(query, character_id, user_id)
        except Exception as e:
            pass  # Error handling, optionally print or raise
            raise
    async def get_all_characters(self) -> list:
        """Fetch all characters from the database as a list of dicts."""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM characters")
                return [dict(row) for row in rows]
        except Exception as e:
            pass  # Error fetching all characters
            return []

    async def get_store_eligible_characters(self, count: int = 10) -> list:
        """Fetch characters eligible for store offers using PostgreSQL aggregation.
        Excludes Supreme rarity, is_video=True characters, and specific excluded IDs."""
        try:
            async with self.pool.acquire() as conn:
                # First check if is_video column exists
                column_check = await conn.fetchrow("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'characters' AND column_name = 'is_video'
                """)
                
                # Get total count of eligible characters for debugging
                if column_check:
                    count_query = """
                        SELECT COUNT(*) FROM characters 
                        WHERE rarity != 'Supreme' 
                        AND (is_video IS NULL OR is_video = false OR is_video = 'false')
                        AND character_id NOT IN (531, 664, 678, 849, 853, 877, 957, 1109, 1248, 1305)
                    """
                else:
                    count_query = """
                        SELECT COUNT(*) FROM characters 
                        WHERE rarity != 'Supreme' 
                        AND character_id NOT IN (531, 664, 678, 849, 853, 877, 957, 1109, 1248, 1305)
                    """
                
                total_count = await conn.fetchval(count_query)
                print(f"[DEBUG] get_store_eligible_characters: total eligible characters in DB: {total_count}")
                
                if column_check:
                    # Use PostgreSQL aggregation to filter and randomly select characters
                    # Handle is_video field more robustly - it might be NULL or not exist
                    query = """
                        SELECT * FROM characters 
                        WHERE rarity != 'Supreme' 
                        AND (is_video IS NULL OR is_video = false OR is_video = 'false')
                        AND character_id NOT IN (531, 664, 678, 849, 853, 877, 957, 1109, 1248, 1305)
                        ORDER BY RANDOM()
                        LIMIT $1
                    """
                else:
                    # Fallback if is_video column doesn't exist
                    query = """
                        SELECT * FROM characters 
                        WHERE rarity != 'Supreme' 
                        AND character_id NOT IN (531, 664, 678, 849, 853, 877, 957, 1109, 1248, 1305)
                        ORDER BY RANDOM()
                        LIMIT $1
                    """
                
                rows = await conn.fetch(query, count)
                result = [dict(row) for row in rows]
                print(f"[DEBUG] Store eligible characters: {len(result)} found, requested: {count}")
                # Log any characters with is_video=True that might have slipped through
                for char in result:
                    if char.get('is_video'):
                        print(f"[WARNING] Character {char.get('character_id')} has is_video=True but was included in store!")
                return result
        except Exception as e:
            print(f"[ERROR] Failed to fetch store eligible characters: {e}")
            return []

    async def get_all_user_ids(self) -> list:
        """Fetch all user IDs from the database."""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("SELECT user_id FROM users")
                return [row['user_id'] for row in rows]
        except Exception as e:
            pass  # Error fetching all user IDs
            return []

    async def get_characters_by_ids(self, char_ids: list) -> list:
        """Fetch characters by a list of character IDs."""
        if not char_ids:
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM characters WHERE character_id = ANY($1::int[])",
                    char_ids
                )
                return [dict(row) for row in rows]
        except Exception as e:
            pass  # Error fetching characters by ids
            return []
    def __init__(self):
        self.pool = _pg_pool
        # Add collection-like attributes for compatibility with MongoDB interface
        self.users = self  # Use self for user operations
        self.characters = self  # Use self for character operations
        self.chat_settings = self  # Use self for chat settings operations
        self.claim_settings = self  # Use self for claim settings operations
        self.drop_settings = self  # Use self for drop settings operations
        self.propose_settings = self  # Use self for propose settings operations
        self.redeem_codes = self  # Use self for redeem codes operations
        
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user data by ID"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if not row:
                return None
            user = dict(row)
            # Ensure last_propose is always an ISO string if present
            if 'last_propose' in user and user['last_propose']:
                if isinstance(user['last_propose'], datetime):
                    user['last_propose'] = user['last_propose'].isoformat()
                else:
                    user['last_propose'] = str(user['last_propose'])
            
            # Parse active_action if it's stored as JSON string
            if 'active_action' in user and isinstance(user['active_action'], str):
                try:
                    import json
                    user['active_action'] = json.loads(user['active_action'])
                except (json.JSONDecodeError, TypeError):
                    user['active_action'] = None
            
            # Parse collection_history if it's stored as JSON string
            if 'collection_history' in user and isinstance(user['collection_history'], str):
                try:
                    import json
                    user['collection_history'] = json.loads(user['collection_history'])
                except (json.JSONDecodeError, TypeError):
                    user['collection_history'] = []
            
            # Parse store_offer if it's stored as JSON string
            if 'store_offer' in user and isinstance(user['store_offer'], str):
                try:
                    import json
                    user['store_offer'] = json.loads(user['store_offer'])
                except (json.JSONDecodeError, TypeError):
                    user['store_offer'] = {}
            
            # Parse supreme_store_offer if it's stored as JSON string
            if 'supreme_store_offer' in user and isinstance(user['supreme_store_offer'], str):
                try:
                    import json
                    user['supreme_store_offer'] = json.loads(user['supreme_store_offer'])
                except (json.JSONDecodeError, TypeError):
                    user['supreme_store_offer'] = {}
            
            return user

    async def insert_redeem_code(self, redeem_data: dict):
        """Insert a new redeem code record."""
        # First ensure the table schema is correct
        try:
            await self.fix_redeem_codes_table()
        except Exception as e:
            print(f"⚠️ Could not fix redeem_codes table: {e}")
        
        # Normalize created_at
        created_at_value = redeem_data.get('created_at')
        if isinstance(created_at_value, str):
            try:
                created_at_value = datetime.fromisoformat(created_at_value)
            except Exception:
                created_at_value = None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO redeem_codes (
                    code, type, character_id, token_amount, shard_amount,
                    created_by, created_at, max_claims, claims, claimed_by
                ) VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, CURRENT_TIMESTAMP), $8, $9, $10)
                ON CONFLICT (code) DO NOTHING
                """,
                redeem_data.get('code'),
                redeem_data.get('type'),
                redeem_data.get('character_id'),
                redeem_data.get('token_amount'),
                redeem_data.get('shard_amount'),
                redeem_data.get('created_by'),
                created_at_value,
                redeem_data.get('max_claims', 1),
                redeem_data.get('claims', 0),
                redeem_data.get('claimed_by', [])
            )

    async def get_redeem_code(self, code: str) -> Optional[Dict]:
        """Fetch a redeem code by code."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM redeem_codes WHERE code = $1",
                code
            )
            return dict(row) if row else None

    async def update_redeem_code_claim(self, code: str, user_id: int) -> bool:
        """Increment claims and append user_id to claimed_by if not already present and under max_claims."""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE redeem_codes
                SET claims = claims + 1,
                    claimed_by = CASE 
                        WHEN claimed_by IS NULL THEN ARRAY[$2::bigint]
                        WHEN NOT ($2 = ANY(claimed_by)) THEN array_append(claimed_by, $2::bigint)
                        ELSE claimed_by
                    END
                WHERE code = $1
                  AND claims < max_claims
                  AND NOT ($2 = ANY(COALESCE(claimed_by, ARRAY[]::bigint[])))
                """,
                code, user_id
            )
            # result like 'UPDATE 1' if updated
            try:
                return result.split()[-1] == '1'
            except Exception:
                return False

    async def add_user(self, user_data: dict):
        """Add a new user"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO users (
                        user_id, username, first_name, last_name, wallet, shards
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        updated_at = CURRENT_TIMESTAMP
                """, user_data['user_id'], user_data.get('username'),
                     user_data.get('first_name'), user_data.get('last_name'),
                     user_data.get('wallet', 0), user_data.get('shards', 0))
        except Exception as e:
            pass  # Error adding user
    
    async def update_user(self, user_id: int, update_data: dict):
        """Update user data"""
        import json
        set_clauses = []
        params = []
        idx = 1
        from datetime import datetime
        
        def convert(obj):
            if isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(i) for i in obj]
            elif hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                return obj
        
        for key, value in update_data.items():
            # Serialize dicts and lists for JSONB columns
            if key in ['active_action', 'collection_preferences'] and isinstance(value, dict):
                value = json.dumps(convert(value))
            elif key == 'collection_history' and isinstance(value, list):
                value = json.dumps(convert(value))
            elif key == 'store_offer' and isinstance(value, dict):
                value = json.dumps(convert(value))
            elif key == 'supreme_store_offer' and isinstance(value, dict):
                value = json.dumps(convert(value))
            elif key == 'last_propose' and value:
                if isinstance(value, str):
                    try:
                        value = datetime.fromisoformat(value)
                    except Exception:
                        pass  # If conversion fails, keep as is
            
            set_clauses.append(f"{key} = ${idx}")
            params.append(value)
            idx += 1
        if not set_clauses:
            return False
        sql = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ${idx}"
        params.append(user_id)
        async with self.pool.acquire() as conn:
            await conn.execute(sql, *params)
        return True
    
    async def get_character(self, char_id: int) -> Optional[Dict]:
        """Get character data by ID"""
        # Check cache first
        if char_id in _character_cache:
            _performance_stats['cache_hits'] += 1
            return _character_cache[char_id]
        
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM characters WHERE character_id = $1",
                    char_id
                )
                if row:
                    character_data = dict(row)
                    _character_cache[char_id] = character_data
                    _performance_stats['cache_hits'] += 1
                    return character_data
                return None
        except Exception as e:
            pass  # Error getting character
            return None
    
    async def get_user_collection(self, user_id: int) -> List[Dict]:
        """Get user's character collection"""
        try:
            async with self.pool.acquire() as conn:
                # First get the user's character IDs from the characters array
                user_result = await conn.fetchrow(
                    "SELECT characters FROM users WHERE user_id = $1",
                    user_id
                )
                
                if not user_result or not user_result[0]:
                    return []
                
                character_ids = user_result[0]
                if not character_ids:
                    return []
                
                # Get character details for all character IDs
                char_ids_tuple = tuple(character_ids)
                if len(char_ids_tuple) == 1:
                    char_ids_tuple = (char_ids_tuple[0],)
                
                rows = await conn.fetch("""
                    SELECT character_id, name, rarity, img_url, file_id, is_video
                    FROM characters 
                    WHERE character_id = ANY($1::int[])
                    ORDER BY character_id
                """, char_ids_tuple)
                
                # Convert to list of dictionaries and add count information
                char_counts = {}
                for char_id in character_ids:
                    char_counts[char_id] = char_counts.get(char_id, 0) + 1
                
                collection = []
                for row in rows:
                    char_dict = dict(row)
                    char_dict['count'] = char_counts.get(char_dict['character_id'], 1)
                    collection.append(char_dict)
                
                return collection
        except Exception as e:
            pass  # Error getting user collection
            return []
    
    async def add_character_to_user(self, user_id: int, character_id: int, collected_at: datetime = None, source: str = 'collected'):
        import json
        if collected_at is None:
            # Always use UTC time
            collected_at = datetime.utcnow()
        else:
            # If a timestamp is provided, ensure it's in UTC
            if collected_at.tzinfo is not None:
                collected_at = collected_at.astimezone(pytz.UTC).replace(tzinfo=None)
        
        # Always ensure collected_at is an ISO string in UTC
        if isinstance(collected_at, datetime):
            collected_at_str = collected_at.isoformat()
        else:
            # fallback: use current UTC time
            collected_at_str = datetime.utcnow().isoformat()
        
        entry = {
            "character_id": character_id,
            "collected_at": collected_at_str,
            "source": source
        }
        async with self.pool.acquire() as conn:
            # Add character to user's collection
            await conn.execute("""
                UPDATE users 
                SET characters = CASE 
                    WHEN characters IS NULL THEN ARRAY[$2::integer] 
                    ELSE array_append(characters, $2::integer) 
                END
                WHERE user_id = $1
            """, user_id, character_id)

            # Append to collection_history - handle both NULL and object cases
            await conn.execute("""
                UPDATE users
                SET collection_history = 
                    CASE 
                        WHEN collection_history IS NULL OR jsonb_typeof(collection_history) != 'array' THEN 
                            to_jsonb(ARRAY[$2::jsonb])
                        ELSE 
                            collection_history || to_jsonb(ARRAY[$2::jsonb])
                    END
                WHERE user_id = $1
            """, user_id, json.dumps(entry))
    async def remove_character_from_user(self, user_id: int, character_id: int):
        """Remove character from user's collection"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    DELETE FROM user_characters 
                    WHERE user_id = $1 AND character_id = $2
                """, user_id, character_id)
                
                # Update user's character array
                await conn.execute("""
                    UPDATE users 
                    SET characters = array_remove(characters, $2)
                    WHERE user_id = $1
                """, user_id, character_id)
                
        except Exception as e:
            pass  # Error removing character from user
    async def remove_single_character_from_user(self, user_id: int, character_id: int):
        """Remove a single instance of character_id from user's characters array (not all)."""
        try:
            async with self.pool.acquire() as conn:
                # Get current characters array
                row = await conn.fetchrow("SELECT characters FROM users WHERE user_id = $1", user_id)
                if not row or not row['characters']:
                    return False
                chars = list(row['characters'])
                if character_id in chars:
                    chars.remove(character_id)
                    await conn.execute("UPDATE users SET characters = $1 WHERE user_id = $2", chars, user_id)
                    return True
                return False
        except Exception as e:
            pass  # Error removing single character
            return False
    
    async def get_random_character(self, locked_rarities=None):
        """Get a random character"""
        try:
            if locked_rarities is None:
                locked_rarities = []
            
            async with self.pool.acquire() as conn:
                if locked_rarities:
                    row = await conn.fetchrow("""
                        SELECT * FROM characters 
                        WHERE rarity NOT IN (SELECT unnest($1::text[]))
                        ORDER BY RANDOM()
                        LIMIT 1
                    """, locked_rarities)
                else:
                    row = await conn.fetchrow("""
                        SELECT * FROM characters 
                        ORDER BY RANDOM()
                        LIMIT 1
                    """)
                
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            pass  # Error getting random character
            return None
    
    async def get_random_character_by_rarities(self, rarities: list) -> dict:
        """Get a random character from specific rarities"""
        if not rarities:
            return None
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT * FROM characters 
                    WHERE rarity = ANY($1::text[])
                    ORDER BY RANDOM()
                    LIMIT 1
                """, rarities)
                
                if row:
                    return dict(row)
                return None
        except Exception as e:
            pass  # Error getting random character by rarities
            return None
    
    async def get_multiple_random_characters_by_rarity(self, rarity: str, count: int = 2) -> list:
        """Get multiple random characters of a specific rarity"""
        if count <= 0:
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT * FROM characters 
                    WHERE rarity = $1
                    ORDER BY RANDOM()
                    LIMIT $2
                """, rarity, count)
                
                result = [dict(row) for row in rows]
                return result
        except Exception as e:
            return []
    
    async def get_all_characters_by_rarity(self, rarity: str) -> list:
        """Get all characters of a specific rarity"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT * FROM characters 
                    WHERE rarity = $1
                    ORDER BY name
                """, rarity)
                
                result = [dict(row) for row in rows]
                return result
        except Exception as e:
            return []
    
    async def get_random_character_by_rarities_excluding(self, excluded_rarities: list, count: int = 1) -> list:
        """Get random characters excluding specific rarities"""
        if count <= 0:
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT * FROM characters 
                    WHERE rarity NOT IN (SELECT unnest($1::text[]))
                    ORDER BY RANDOM()
                    LIMIT $2
                """, excluded_rarities, count)
                
                return [dict(row) for row in rows]
        except Exception as e:
            pass  # Error getting random characters by rarities excluding
            return []
    
    async def get_propose_settings(self):
        """Get propose settings"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM propose_settings LIMIT 1")
                if row:
                    settings = dict(row)
                    # Parse JSON fields if they are strings
                    if isinstance(settings.get('propose_weights'), str):
                        settings['propose_weights'] = json.loads(settings['propose_weights'])
                    if isinstance(settings.get('rarity_rates'), str):
                        settings['rarity_rates'] = json.loads(settings['rarity_rates'])
                    return settings
                
                # Create default settings if none exist
                default_settings = {
                    'locked_rarities': ['Common', 'Medium'],
                    'propose_cooldown': 100,
                    'propose_cost': 20000,
                    'acceptance_rate': 50,
                    'propose_weights': {
                        'Common': 30,
                        'Medium': 25,
                        'Rare': 20,
                        'Legendary': 15,
                        'Exclusive': 10,
                        'Elite': 5,
                        'Limited Edition': 3,
                        'Ultimate': 2,
                        'Supreme': 1,
                        'Zenith': 1,
                        'Mythic': 1,
                        'Ethereal': 1,
                        'Premium': 1
                    },
                    'rarity_rates': {
                        'Common': 80,
                        'Medium': 70,
                        'Rare': 60,
                        'Legendary': 50,
                        'Exclusive': 40,
                        'Elite': 30,
                        'Limited Edition': 20,
                        'Ultimate': 15,
                        'Supreme': 10,
                        'Zenith': 8,
                        'Mythic': 5,
                        'Ethereal': 3,
                        'Premium': 2
                    }
                }
                
                # Insert default settings
                await conn.execute("""
                    INSERT INTO propose_settings (
                        locked_rarities, propose_cooldown, propose_cost, acceptance_rate,
                        propose_weights, rarity_rates
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                """, 
                default_settings['locked_rarities'],
                default_settings['propose_cooldown'],
                default_settings['propose_cost'],
                default_settings['acceptance_rate'],
                json.dumps(default_settings['propose_weights']),
                json.dumps(default_settings['rarity_rates'])
                )
                
                return default_settings
                
        except Exception as e:
            pass  # Error getting propose settings
            return None
    
    async def update_propose_settings(self, settings: Dict[str, Any]):
        """Update propose settings"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE propose_settings 
                    SET locked_rarities = $1, propose_cooldown = $2, propose_cost = $3,
                        acceptance_rate = $4, propose_weights = $5, rarity_rates = $6,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, 
                settings.get('locked_rarities', []),
                settings.get('propose_cooldown', 100),
                settings.get('propose_cost', 20000),
                settings.get('acceptance_rate', 50),
                json.dumps(settings.get('propose_weights', {})),
                json.dumps(settings.get('rarity_rates', {}))
                )
                
        except Exception as e:
            pass  # Error updating propose settings
            # If the update fails, try to insert a new record
            try:
                await conn.execute("""
                    INSERT INTO propose_settings (
                        locked_rarities, propose_cooldown, propose_cost, acceptance_rate,
                        propose_weights, rarity_rates
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (id) DO UPDATE SET
                        locked_rarities = EXCLUDED.locked_rarities,
                        propose_cooldown = EXCLUDED.propose_cooldown,
                        propose_cost = EXCLUDED.propose_cost,
                        acceptance_rate = EXCLUDED.acceptance_rate,
                        propose_weights = EXCLUDED.propose_weights,
                        rarity_rates = EXCLUDED.rarity_rates,
                        updated_at = CURRENT_TIMESTAMP
                """, 
                settings.get('locked_rarities', []),
                settings.get('propose_cooldown', 100),
                settings.get('propose_cost', 20000),
                settings.get('acceptance_rate', 50),
                json.dumps(settings.get('propose_weights', {})),
                json.dumps(settings.get('rarity_rates', {}))
                )
            except Exception as insert_error:
                pass  # Error inserting propose settings
    
    async def get_claim_settings(self) -> Optional[Dict[str, Any]]:
        """Get claim settings from database"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM claim_settings LIMIT 1")
                if row:
                    settings = dict(row)
                    # Parse JSON fields if they are strings
                    if isinstance(settings.get('settings'), str):
                        settings['settings'] = json.loads(settings['settings'])
                    return settings
                
                # Create default settings if none exist
                default_settings = {
                    'locked_rarities': [],
                    'claim_cooldown': 24,
                    'settings': {}
                }
                
                await conn.execute("""
                    INSERT INTO claim_settings (locked_rarities, claim_cooldown, settings)
                    VALUES ($1, $2, $3)
                """, default_settings['locked_rarities'], default_settings['claim_cooldown'], json.dumps(default_settings['settings']))
                
                return default_settings
                
        except Exception as e:
            pass  # Error getting claim settings
            return None
    
    async def update_claim_settings(self, settings: Dict[str, Any]):
        """Update claim settings in database"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE claim_settings 
                    SET locked_rarities = $1, claim_cooldown = $2, settings = $3, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, settings.get('locked_rarities', []), settings.get('claim_cooldown', 24), json.dumps(settings.get('settings', {})))
                
        except Exception as e:
            pass  # Error updating claim settings
    
    async def log_user_transaction(self, user_id: int, action_type: str, details: dict):
        """Log user transaction"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO user_transactions (user_id, action_type, details, created_at)
                    VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                """, user_id, action_type, json.dumps(details))
                
        except Exception as e:
            pass  # Error handling, optionally print or raise
    
    async def get_drop_settings(self):
        """Get drop settings"""
        # Check cache first
        if 'drop_settings' in _drop_settings_cache:
            return _drop_settings_cache['drop_settings']
        
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM drop_settings LIMIT 1")
                if row:
                    settings = dict(row)
                    # Parse JSON fields if they are strings
                    if isinstance(settings.get('rarity_weights'), str):
                        settings['rarity_weights'] = json.loads(settings['rarity_weights'])
                    if isinstance(settings.get('daily_limits'), str):
                        settings['daily_limits'] = json.loads(settings['daily_limits'])
                    _drop_settings_cache['drop_settings'] = settings
                    return settings
                return None
        except Exception as e:
            logger.error(f"Error getting drop settings: {e}")
            return None
    
    async def update_drop_settings(self, settings: dict):
        """Update drop settings"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE drop_settings 
                    SET rarity_weights = $1, daily_limits = $2, 
                        locked_rarities = $3, drop_frequency = $4,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, json.dumps(settings.get('rarity_weights', {})),
                     json.dumps(settings.get('daily_limits', {})),
                     settings.get('locked_rarities', []),
                     settings.get('drop_frequency', 300))
                
                # Clear cache
                _drop_settings_cache.clear()
                
        except Exception as e:
            logger.error(f"Error updating drop settings: {e}")
    
    async def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Get user statistics"""
        # Check cache first
        if user_id in _user_stats_cache:
            return _user_stats_cache[user_id]
        
        try:
            async with self.pool.acquire() as conn:
                # Get user data
                user_row = await conn.fetchrow("""
                    SELECT wallet, shards, characters, created_at
                    FROM users WHERE user_id = $1
                """, user_id)
                
                if not user_row:
                    return {}
                
                user_data = dict(user_row)
                character_count = len(user_data.get('characters', []))
                
                # Get collection stats by rarity
                rarity_stats = await conn.fetch("""
                    SELECT c.rarity, COUNT(*) as count
                    FROM user_characters uc
                    JOIN characters c ON uc.character_id = c.character_id
                    WHERE uc.user_id = $1
                    GROUP BY c.rarity
                """, user_id)
                
                stats = {
                    'user_id': user_id,
                    'wallet': user_data.get('wallet', 0),
                    'shards': user_data.get('shards', 0),
                    'total_characters': character_count,
                    'rarity_breakdown': {row['rarity']: row['count'] for row in rarity_stats},
                    'created_at': user_data.get('created_at')
                }
                
                _user_stats_cache[user_id] = stats
                return stats
                
        except Exception as e:
            logger.error(f"Error getting user stats {user_id}: {e}")
            return {}
    
    async def get_leaderboard(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get leaderboard"""
        # Check cache first
        cache_key = f'leaderboard_{limit}'
        if cache_key in _leaderboard_cache:
            return _leaderboard_cache[cache_key]
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT u.user_id, u.first_name, u.username, 
                           u.wallet, u.shards, array_length(u.characters, 1) as character_count
                    FROM users u
                    WHERE u.is_banned = FALSE
                    ORDER BY array_length(u.characters, 1) DESC, u.wallet DESC
                    LIMIT $1
                """, limit)
                
                leaderboard = [dict(row) for row in rows]
                _leaderboard_cache[cache_key] = leaderboard
                return leaderboard
                
        except Exception as e:
            logger.error(f"Error getting leaderboard: {e}")
            return []
    
    async def get_chat_settings(self, chat_id: int):
        """Get chat settings"""
        # Check cache first
        if chat_id in _chat_settings_cache:
            return _chat_settings_cache[chat_id]
        
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT * FROM chat_settings WHERE chat_id = $1
                """, chat_id)
                
                if row:
                    settings = dict(row)
                    _chat_settings_cache[chat_id] = settings
                    return settings
                return None
                
        except Exception as e:
            logger.error(f"Error getting chat settings {chat_id}: {e}")
            return None
    
    async def update_chat_settings(self, chat_id: int, settings: dict):
        """Update chat settings"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO chat_settings (chat_id, chat_title, drop_enabled, drop_interval, last_drop)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (chat_id) DO UPDATE SET
                        chat_title = EXCLUDED.chat_title,
                        drop_enabled = EXCLUDED.drop_enabled,
                        drop_interval = EXCLUDED.drop_interval,
                        last_drop = EXCLUDED.last_drop,
                        updated_at = CURRENT_TIMESTAMP
                """, chat_id, settings.get('chat_title'),
                     settings.get('drop_enabled', True),
                     settings.get('drop_interval', 300),
                     settings.get('last_drop'))
                
                # Clear cache
                if chat_id in _chat_settings_cache:
                    del _chat_settings_cache[chat_id]
                    
        except Exception as e:
            logger.error(f"Error updating chat settings {chat_id}: {e}")
    
    async def is_banned(self, user_id: int) -> bool:
        """Check if user is banned"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT is_banned FROM users WHERE user_id = $1
                """, user_id)
                return row and row['is_banned']
        except Exception as e:
            logger.error(f"Error checking ban status for user {user_id}: {e}")
            return False
    
    async def ban_user(self, user_id: int, permanent: bool = False, duration_minutes: int = 10):
        """Ban a user"""
        try:
            async with self.pool.acquire() as conn:
                if permanent:
                    await conn.execute("""
                        UPDATE users 
                        SET is_banned = TRUE, banned_at = CURRENT_TIMESTAMP
                        WHERE user_id = $1
                    """, user_id)
                else:
                    await conn.execute("""
                        UPDATE users 
                        SET last_temp_ban = CURRENT_TIMESTAMP
                        WHERE user_id = $1
                    """, user_id)
            return True
        except Exception as e:
            logger.error(f"Error banning user {user_id}: {e}")
            return False
    
    async def unban_user(self, user_id: int):
        """Unban a user"""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    "UPDATE users SET is_banned = FALSE WHERE user_id = $1",
                    user_id
                )
                # Check if any row was affected
                return result.split()[-1] != '0'  # Returns True if rows were affected
        except Exception as e:
            logger.error(f"Error unbanning user {user_id}: {e}")
            return False
    
    async def remove_sudo(self, user_id: int):
        """Remove sudo privileges from a user"""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    "UPDATE users SET sudo = FALSE WHERE user_id = $1",
                    user_id
                )
                # Check if any row was affected
                return result.split()[-1] != '0'  # Returns True if rows were affected
        except Exception as e:
            logger.error(f"Error removing sudo from user {user_id}: {e}")
            return False
    
    async def remove_og(self, user_id: int):
        """Remove OG status from a user"""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    "UPDATE users SET og = FALSE WHERE user_id = $1",
                    user_id
                )
                # Check if any row was affected
                return result.split()[-1] != '0'  # Returns True if rows were affected
        except Exception as e:
            logger.error(f"Error removing OG status from user {user_id}: {e}")
            return False
    
    async def get_user_preferences(self, user_id: int) -> Dict[str, Any]:
        """Get user preferences"""
        import json
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(
                "SELECT collection_preferences FROM users WHERE user_id = $1",
                user_id
            )
            if result and result[0]:
                try:
                    # Parse JSON string if it's a string
                    if isinstance(result[0], str):
                        return json.loads(result[0])
                    # If it's already a dict, return as is
                    elif isinstance(result[0], dict):
                        return result[0]
                    else:
                        return {
                            'mode': 'default',
                            'filter': None
                        }
                except (json.JSONDecodeError, TypeError):
                    return {
                        'mode': 'default',
                        'filter': None
                    }
            return {
                'mode': 'default',
                'filter': None
            }
    
    async def update_user_preferences(self, user_id: int, preferences: Dict[str, Any]):
        """Update user preferences"""
        import json
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET collection_preferences = $1 WHERE user_id = $2",
                json.dumps(preferences), user_id
            )
    
    async def close(self):
        """Close the database connection"""
        global _pg_pool
        if _pg_pool:
            await _pg_pool.close()
            _pg_pool = None
    
    # MongoDB-style methods for compatibility
    async def find_one(self, query: dict, projection: dict = None) -> Optional[Dict]:
        """MongoDB-style find_one method"""
        if 'user_id' in query:
            return await self.get_user(query['user_id'])
        elif 'character_id' in query:
            return await self.get_character(query['character_id'])
        elif 'chat_id' in query:
            return await self.get_chat_settings(query['chat_id'])
        elif 'code' in query:
            return await self.get_redeem_code(query['code'])
        return None
    
    async def update_one(self, query: dict, update: dict):
        """MongoDB-style update_one method for users table. Supports $set, $inc, $push. Handles nested JSONB keys."""
        if 'user_id' not in query:
            raise ValueError("update_one only supports user_id queries for now")
        user_id = query['user_id']
        set_fields = update.get('$set', {})
        inc_fields = update.get('$inc', {})
        push_fields = update.get('$push', {})
        updates = []
        params = []
        # $set
        for k, v in set_fields.items():
            param_idx = len(params) + 1
            if '.' in k:
                field, subkey = k.split('.', 1)
                # Always cast to text for jsonb_set to avoid polymorphic type errors
                updates.append(f"{field} = jsonb_set(COALESCE({field}, '{{}}'::jsonb), '{{{subkey}}}', to_jsonb(${param_idx}::text), true)")
                params.append(v if isinstance(v, str) else str(v))
            else:
                updates.append(f"{k} = ${param_idx}")
                params.append(v)
        # $inc
        for k, v in inc_fields.items():
            param_idx = len(params) + 1
            if '.' in k:
                field, subkey = k.split('.', 1)
                updates.append(f"{field} = jsonb_set(COALESCE({field}, '{{}}'::jsonb), '{{{subkey}}}', to_jsonb((COALESCE(({field}->>'{subkey}')::int, 0) + ${param_idx})), true)")
                params.append(v)
            else:
                updates.append(f"{k} = {k} + ${param_idx}")
                params.append(v)
        # $push (for JSONB arrays and integer[] arrays)
        for k, v in push_fields.items():
            param_idx = len(params) + 1
            # Handle $each for arrays
            if isinstance(v, dict) and '$each' in v:
                values = v['$each']
                if k == 'characters':
                    # For integer[] fields, use array concatenation
                    updates.append(f"{k} = COALESCE({k}, ARRAY[]::integer[]) || ${param_idx}::integer[]")
                    params.append(values)
                else:
                    # Default to jsonb array append - convert to JSON string first
                    import json
                    def convert_for_json(obj):
                        if isinstance(obj, dict):
                            return {k: convert_for_json(v) for k, v in obj.items()}
                        elif isinstance(obj, list):
                            return [convert_for_json(i) for i in obj]
                        elif hasattr(obj, 'isoformat'):
                            return obj.isoformat()
                        else:
                            return obj
                    
                    json_values = json.dumps(convert_for_json(values))
                    updates.append(f"{k} = COALESCE({k}, '[]'::jsonb) || ${param_idx}::jsonb")
                    params.append(json_values)
            else:
                if '.' in k:
                    field, subkey = k.split('.', 1)
                    # Always cast to text for jsonb_set to avoid polymorphic type errors
                    updates.append(f"{field} = jsonb_set(COALESCE({field}, '{{}}'::jsonb), '{{{subkey}}}', (COALESCE({field}->'{subkey}', '[]'::jsonb) || to_jsonb(${param_idx}::text)), true)")
                    params.append(v if isinstance(v, str) else str(v))
                elif k == 'characters':
                    # For integer[] fields, use array_append
                    updates.append(f"{k} = array_append(COALESCE({k}, ARRAY[]::integer[]), ${param_idx}::integer)")
                    params.append(v)
                else:
                    # Default to jsonb array append
                    updates.append(f"{k} = COALESCE({k}, '[]'::jsonb) || to_jsonb(${param_idx})")
                    params.append(v)
        if not updates:
            return False
        param_idx = len(params) + 1
        sql = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ${param_idx}"
        params.append(user_id)
        async with self.pool.acquire() as conn:
            await conn.execute(sql, *params)
        return True
    
    async def find(self, query: dict = None, projection: dict = None):
        """MongoDB-style find method - returns a cursor-like object"""
        return PostgresCursor(self, query, projection)
    
    async def count_documents(self, query: dict = None) -> int:
        """MongoDB-style count_documents method"""
        if query is None:
            query = {}
        
        if 'user_id' in query:
            # Count users
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow("SELECT COUNT(*) FROM users WHERE user_id = $1", query['user_id'])
                return result[0] if result else 0
        elif 'character_id' in query:
            # Count characters
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow("SELECT COUNT(*) FROM characters WHERE character_id = $1", query['character_id'])
                return result[0] if result else 0
        elif 'is_video' in query:
            # Count video characters
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow("SELECT COUNT(*) FROM characters WHERE is_video = $1", query['is_video'])
                return result[0] if result else 0
        elif 'rarity' in query:
            # Count characters by rarity
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow("SELECT COUNT(*) FROM characters WHERE rarity = $1", query['rarity'])
                return result[0] if result else 0
        elif 'characters' in query:
            # Count users with specific character
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow("SELECT COUNT(*) FROM users WHERE $1 = ANY(characters::int[])", query['characters'])
                return result[0] if result else 0
        else:
            # Count all records in the appropriate table
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow("SELECT COUNT(*) FROM users")
                return result[0] if result else 0
    
    def aggregate(self, pipeline: list):
        """MongoDB-style aggregate method"""
        return PostgresAggregationCursor(self, pipeline)
    
    async def estimated_document_count(self) -> int:
        """MongoDB-style estimated_document_count method"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow("SELECT COUNT(*) FROM users")
            return result[0] if result else 0
        
    async def get_character_collectors(self, character_id: int) -> list:
        """Return a list of users who have collected the given character_id."""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT user_id, characters FROM users WHERE $1 = ANY(characters)",
                    character_id
                )
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error in get_character_collectors for character {character_id}: {e}")
            return []
        
    async def get_top_collectors(self, character_id: int, limit: int = 5) -> list:
        """Return the top users who have collected the given character_id, with name, username, and count."""
        try:
            char_id_int = int(character_id)
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT user_id, first_name AS name, username,
                        (SELECT COUNT(*) FROM unnest(characters) AS c WHERE c = $1) AS count
                    FROM users
                    WHERE $1 = ANY(characters)
                    ORDER BY count DESC
                    LIMIT $2
                    """,
                    char_id_int, limit
                )
                return [dict(row) for row in rows if row['count'] > 0]
        except Exception as e:
            logger.error(f"Error in get_top_collectors for character {character_id}: {e}")
            return []

    async def get_group_collectors(self, chat_id: int, character_id: int) -> list:
        """Return a list of users in a specific group who have collected the given character_id."""
        try:
            char_id_int = int(character_id)
            async with self.pool.acquire() as conn:
                # First check if groups column exists, if not return empty list
                column_check = await conn.fetchrow("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'users' AND column_name = 'groups'
                """)
                
                if not column_check:
                    # If groups column doesn't exist, return empty list
                    return []
                
                # Get users in the group who have collected this character
                rows = await conn.fetch(
                    """
                    SELECT user_id, first_name AS name, username,
                        (SELECT COUNT(*) FROM unnest(characters) AS c WHERE c = $1) AS count
                    FROM users
                    WHERE $1 = ANY(characters)
                    AND $2::bigint = ANY(groups)
                    ORDER BY count DESC
                    """,
                    char_id_int, chat_id
                )
                return [dict(row) for row in rows if row['count'] > 0]
        except Exception as e:
            logger.error(f"Error in get_group_collectors for character {character_id} in group {chat_id}: {e}")
            return []
    
    async def get_todays_top_collectors(self, limit: int = 10):
        """Get today's top collectors efficiently using JSONB queries."""
        try:
            # Use UTC only for simplicity and consistency
            from datetime import datetime, timedelta
            
            # Get current UTC time and calculate today's boundaries
            utc_now = datetime.utcnow()
            today_utc = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_utc = today_utc + timedelta(days=1)
            
            query = """
            SELECT 
                u.user_id,
                u.first_name,
                COUNT(*) as today_count
            FROM users u,
            LATERAL jsonb_array_elements(
                CASE 
                    WHEN jsonb_typeof(u.collection_history) = 'array' THEN 
                        COALESCE(u.collection_history, '[]'::jsonb)
                    ELSE 
                        '[]'::jsonb
                END
            ) AS entry
            WHERE 
                entry->>'source' = 'collected'
                AND entry->>'collected_at' IS NOT NULL
                AND (
                    -- Handle both timestamp and string formats
                    CASE 
                        WHEN jsonb_typeof(entry->'collected_at') = 'string' THEN
                            (entry->>'collected_at')::timestamp >= $1
                            AND (entry->>'collected_at')::timestamp < $2
                        ELSE
                            (entry->>'collected_at')::timestamp >= $1
                            AND (entry->>'collected_at')::timestamp < $2
                    END
                )
            GROUP BY u.user_id, u.first_name
            ORDER BY today_count DESC
            LIMIT $3
            """
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, today_utc, tomorrow_utc, limit)
            
            collectors = []
            for row in rows:
                collectors.append({
                    'first_name': row['first_name'] or 'Unknown',
                    'user_id': row['user_id'],
                    'count': row['today_count']
                })
            
            return collectors
            
        except Exception as e:
            logger.error(f"Error getting today's top collectors: {e}")
            return []

    async def create_collection_history_indexes(self):
        """Manually create collection_history indexes for existing databases."""
        try:
            async with self.pool.acquire() as conn:
                await self._create_collection_history_indexes(conn)
                logger.info("Collection history indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating collection history indexes: {e}")
            raise

    async def fix_redeem_codes_table(self):
        """Manually fix the redeem_codes table schema if it's missing columns."""
        try:
            async with self.pool.acquire() as conn:
                # Check if table exists
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'redeem_codes'
                    );
                """)
                
                if not table_exists:
                    # Create table from scratch
                    await conn.execute('''
                        CREATE TABLE redeem_codes (
                            code TEXT PRIMARY KEY,
                            type TEXT,
                            character_id INTEGER,
                            token_amount INTEGER,
                            shard_amount INTEGER,
                            created_by BIGINT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            max_claims INTEGER NOT NULL DEFAULT 1,
                            claims INTEGER NOT NULL DEFAULT 0,
                            claimed_by BIGINT[] DEFAULT ARRAY[]::BIGINT[]
                        );
                    ''')
                    print("✅ Created redeem_codes table from scratch")
                    return True
                
                # Check and add missing columns
                columns_to_check = [
                    ('shard_amount', 'INTEGER'),
                    ('token_amount', 'INTEGER'),
                    ('type', 'TEXT'),
                    ('character_id', 'INTEGER'),
                    ('created_by', 'BIGINT'),
                    ('created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
                    ('max_claims', 'INTEGER NOT NULL DEFAULT 1'),
                    ('claims', 'INTEGER NOT NULL DEFAULT 0'),
                    ('claimed_by', 'BIGINT[] DEFAULT ARRAY[]::BIGINT[]')
                ]
                
                for column_name, column_type in columns_to_check:
                    column_exists = await conn.fetchval("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns 
                            WHERE table_name = 'redeem_codes' AND column_name = $1
                        );
                    """, column_name)
                    
                    if not column_exists:
                        try:
                            await conn.execute(f"ALTER TABLE redeem_codes ADD COLUMN {column_name} {column_type}")
                            print(f"✅ Added missing column: {column_name}")
                        except Exception as e:
                            print(f"⚠️ Could not add column {column_name}: {e}")
                
                print("✅ Redeem codes table schema check completed")
                return True
                
        except Exception as e:
            print(f"❌ Error fixing redeem_codes table: {e}")
            return False

class PostgresCursor:
    """Cursor-like object for PostgreSQL queries"""
    def __init__(self, db, query: dict = None, projection: dict = None):
        self.db = db
        self.query = query or {}
        self.projection = projection or {}
        self.limit_val = None
        self.skip_val = 0
        self.sort_field = None
        self.sort_direction = 1
    
    def limit(self, limit: int):
        self.limit_val = limit
        return self
    
    def skip(self, skip: int):
        self.skip_val = skip
        return self
    
    def sort(self, field: str, direction: int = 1):
        self.sort_field = field
        self.sort_direction = direction
        return self
    
    async def to_list(self, length: int = None):
        """Convert cursor to list"""
        if length is None and self.limit_val:
            length = self.limit_val
        
        # Build SQL query based on MongoDB-style query
        sql = "SELECT * FROM users"  # Default table
        params = []
        param_count = 0
        
        if 'user_id' in self.query:
            param_count += 1
            sql = "SELECT * FROM users WHERE user_id = $" + str(param_count)
            params.append(self.query['user_id'])
        elif 'character_id' in self.query:
            param_count += 1
            sql = "SELECT * FROM characters WHERE character_id = $" + str(param_count)
            params.append(self.query['character_id'])
        elif 'is_video' in self.query:
            param_count += 1
            sql = "SELECT * FROM characters WHERE is_video = $" + str(param_count)
            params.append(self.query['is_video'])
        elif 'rarity' in self.query:
            param_count += 1
            sql = "SELECT * FROM characters WHERE rarity = $" + str(param_count)
            params.append(self.query['rarity'])
        elif 'sudo' in self.query:
            param_count += 1
            sql = "SELECT * FROM users WHERE sudo = $" + str(param_count)
            params.append(self.query['sudo'])
        elif 'og' in self.query:
            param_count += 1
            sql = "SELECT * FROM users WHERE og = $" + str(param_count)
            params.append(self.query['og'])
        
        # Add sorting
        if self.sort_field:
            sql += f" ORDER BY {self.sort_field}"
            if self.sort_direction == -1:
                sql += " DESC"
        
        # Add limit and offset
        if self.limit_val:
            sql += f" LIMIT {self.limit_val}"
        if self.skip_val:
            sql += f" OFFSET {self.skip_val}"
        
        async with self.db.pool.acquire() as conn:
            results = await conn.fetch(sql, *params)
            return [dict(row) for row in results]
        

class PostgresAggregationCursor:
    """Aggregation cursor-like object for PostgreSQL"""
    def __init__(self, db, pipeline: list):
        self.db = db
        self.pipeline = pipeline
    
    async def to_list(self, length: int = None):
        """Convert aggregation cursor to list"""
        try:
            # Handle the specific aggregation pipeline used in vidcollection.py
            if len(self.pipeline) == 2:
                match_stage = self.pipeline[0]
                project_stage = self.pipeline[1]
                
                # Check if this is the character count aggregation
                if (match_stage.get('$match', {}).get('characters') and 
                    project_stage.get('$project', {}).get('count', {}).get('$size', {}).get('$filter')):
                    
                    character_id = match_stage['$match']['characters']
                    
                    # Convert MongoDB aggregation to SQL
                    async with self.db.pool.acquire() as conn:
                        # Count occurrences of this character in each user's collection
                        sql = """
                            SELECT user_id, 
                                   (SELECT COUNT(*) FROM unnest(characters) AS c WHERE c = $1) as count
                            FROM users 
                            WHERE $1 = ANY(characters)
                        """
                        results = await conn.fetch(sql, character_id)
                        return [dict(row) for row in results]
            
            # Default fallback for other aggregations
            return []
            
        except Exception as e:
            logger.error(f"Error in PostgresAggregationCursor.to_list: {e}")
            return []

# Initialize database instance
_db_instance = None

async def ensure_database():
    """Ensure database is initialized (compatibility function)"""
    if _pg_pool is None:
        raise RuntimeError("PostgreSQL pool not initialized. Call init_database() first.")
    return True

async def init_database_instance(postgres_uri: str):
    """Initialize the database instance"""
    global _db_instance
    if _db_instance is None:
        # Initialize the connection pool
        await init_database(postgres_uri)
        _db_instance = PostgresDatabase()
        logger.info("PostgreSQL database initialized successfully")
    return _db_instance

def get_database():
    """Get the database instance"""
    global _db_instance
    if _db_instance is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    return _db_instance

async def close_database():
    """Close database connections"""
    global _db_instance
    if _db_instance:
        await _db_instance.close()
        _db_instance = None

async def restart_connection_pool():
    """Restart the connection pool to clear cached statements"""
    global _pg_pool, _db_instance, _postgres_uri
    
    logger.info("Restarting PostgreSQL connection pool...")
    
    # Close existing pool
    if _pg_pool:
        await _pg_pool.close()
        _pg_pool = None
    
    # Reinitialize pool
    try:
        _pg_pool = await asyncpg.create_pool(
            _postgres_uri,
            min_size=5,
            max_size=20,
            command_timeout=30,
            server_settings={
                'jit': 'off',  # Disable JIT for better performance
                'statement_timeout': '30000',  # 30 seconds
                'idle_in_transaction_session_timeout': '60000'  # 1 minute
            }
        )
        
        # Test connection
        async with _pg_pool.acquire() as conn:
            await conn.execute('SELECT 1')
        
        # Update database instance
        _db_instance = PostgresDatabase()
        
        logger.info("PostgreSQL connection pool restarted successfully")
        
    except Exception as e:
        logger.error(f"Failed to restart PostgreSQL pool: {e}")
        raise