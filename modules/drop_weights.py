from datetime import datetime

async def setup_drop_weights_and_limits(db):
    """Set up drop rarity weights and daily limits"""
    try:
        print("Setting up drop weights and limits...")
        settings = await db.get_drop_settings()
        
        # Set rarity weights - higher numbers mean higher chance of dropping
        # Each tier is roughly 2x rarer than the previous
        rarity_weights = {
            "Common": 1000,     # Base weight (100%)
            "Medium": 500,      # 50% of Common
            "Rare": 250,        # 25% of Common
            "Legendary": 180,   # 12.5% of Common
            "Exclusive": 60,    # 6% of Common
            "Elite": 55,        # 4% of Common
            "Limited Edition": 45,  # 2% of Common
            "Ultimate": 15,     # 1% of Common
            "Supreme": 0,       # 0.5% of Common
            "Mythic": 30,        # 0.2% of Common
            "Zenith": 0,        # 0.1% of Common
            "Ethereal": 0,      # Not dropping
            "Premium": 0        # Not dropping
        }
        
        # Set daily limits - None means no limit, 0 means not dropping
        daily_limits = {
            "Common": None,      # No limit
            "Medium": None,     # No limit
            "Rare": None,       # No limit
            "Legendary": None,    # 20 per day
            "Exclusive": None,    # 10 per day
            "Elite": None,         # 5 per day
            "Limited Edition": None, # 3 per day
            "Ultimate": 2,      # 2 per day
            "Supreme": 0,       # Not dropping
            "Mythic": 3,        # 1 per day
            "Zenith": 0,        # Not dropping
            "Ethereal": 0,      # Not dropping
            "Premium": 0        # Not dropping
        }
        
        # Add dynamic weight adjustment based on time of day
        # This will be used to slightly boost certain rarities during specific hours
        time_weights = {
            "Common": {
                "boost_hours": [0, 1, 2, 3, 4, 5],  # Night hours
                "boost_multiplier": 1.2  # 20% boost
            },
            "Rare": {
                "boost_hours": [12, 13, 14, 15],    # Afternoon hours
                "boost_multiplier": 1.3  # 30% boost
            },
            "Legendary": {
                "boost_hours": [18, 19, 20, 21],    # Evening hours
                "boost_multiplier": 1.4  # 40% boost
            },
            "Ultimate": {
                "boost_hours": [6, 7, 8, 9],        # Morning hours
                "boost_multiplier": 1.5  # 50% boost
            }
        }
        
        # Add rarity progression tracking
        rarity_progression = {
            "last_drop_time": datetime.now().isoformat(),
            "consecutive_common": 0,
            "consecutive_medium": 0,
            "consecutive_rare": 0,
            "consecutive_legendary": 0,
            "consecutive_exclusive": 0,
            "consecutive_elite": 0,
            "consecutive_limited_edition": 0,
            "consecutive_ultimate": 0,
            "consecutive_supreme": 0,
            "consecutive_mythic": 0,
            "consecutive_zenith": 0
        }
        
        # Update settings with all new parameters
        settings['rarity_weights'] = rarity_weights
        settings['daily_limits'] = daily_limits
        settings['time_weights'] = time_weights
        settings['rarity_progression'] = rarity_progression
        
        # Reset daily drops counter
        settings['daily_drops'] = {}
        settings['last_reset_date'] = datetime.now().strftime('%Y-%m-%d')
        
        print(f"Updating drop settings with weights: {rarity_weights}")
        print(f"Daily limits: {daily_limits}")
        print(f"Time weights: {time_weights}")
        await db.update_drop_settings(settings)
        print("Drop settings updated successfully")
        
        return settings
    except Exception as e:
        print(f"Error setting up drop weights and limits: {e}")
        return None 