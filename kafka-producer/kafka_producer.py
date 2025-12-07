import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import uuid


# 153 different action types as mentioned in requirements
ACTION_TYPES = [
    "button_click", "product_view", "add_to_cart", "remove_from_cart", "checkout_start",
    "checkout_complete", "search", "filter_apply", "filter_clear", "sort_change",
    "category_view", "product_detail_view", "wishlist_add", "wishlist_remove",
    "review_submit", "rating_submit", "share_product", "share_app", "login",
    "logout", "register", "profile_edit", "password_change", "address_add",
    "address_edit", "address_delete", "payment_method_add", "payment_method_remove",
    "notification_enable", "notification_disable", "settings_change", "language_change",
    "theme_change", "help_view", "faq_view", "support_contact", "tutorial_start",
    "tutorial_complete", "tutorial_skip", "onboarding_start", "onboarding_complete",
    "promo_code_apply", "promo_code_remove", "coupon_view", "coupon_apply",
    "order_track", "order_cancel", "order_return", "refund_request", "delivery_track",
    "delivery_update", "subscription_start", "subscription_cancel", "subscription_renew",
    "content_view", "content_share", "content_like", "content_comment", "content_bookmark",
    "video_play", "video_pause", "video_complete", "video_skip", "audio_play",
    "audio_pause", "audio_complete", "image_view", "image_zoom", "image_share",
    "map_view", "location_enable", "location_disable", "location_search", "nearby_search",
    "store_locator", "store_view", "store_favorite", "store_unfavorite", "store_review",
    "chat_start", "chat_message", "chat_end", "call_start", "call_end",
    "survey_start", "survey_submit", "survey_skip", "feedback_submit", "bug_report",
    "app_update_check", "app_update_install", "app_crash", "app_performance", "app_launch",
    "app_background", "app_foreground", "screen_view", "screen_exit", "scroll",
    "swipe", "pinch_zoom", "long_press", "double_tap", "gesture",
    "banner_click", "banner_view", "ad_click", "ad_view", "ad_skip",
    "social_login", "social_share", "social_connect", "social_disconnect", "referral_send",
    "referral_accept", "achievement_unlock", "badge_earn", "level_up", "points_earn",
    "points_redeem", "gamification_view", "leaderboard_view", "challenge_start",
    "challenge_complete", "streak_start", "streak_continue", "streak_break", "milestone_reach",
    "feature_discover", "feature_use", "feature_abandon", "upgrade_prompt", "upgrade_accept",
    "upgrade_decline", "trial_start", "trial_end", "subscription_prompt", "subscription_accept",
    "subscription_decline", "push_enable", "push_disable", "email_subscribe", "email_unsubscribe",
    "sms_subscribe", "sms_unsubscribe", "privacy_settings", "data_export", "account_delete"
]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip'
    )


def generate_user_action():
    """Generate a random user action event"""
    user_id = random.randint(1, 30000000)  # 30M devices
    action_type = random.choice(ACTION_TYPES)
    timestamp = datetime.now()
    
    # Generate additional metadata based on action type
    metadata = {}
    
    if action_type in ["product_view", "add_to_cart", "remove_from_cart", "product_detail_view"]:
        metadata["product_id"] = random.randint(1, 10000)
        metadata["product_name"] = f"Product_{metadata['product_id']}"
        metadata["category"] = random.choice(["Electronics", "Clothing", "Food", "Books", "Toys"])
        metadata["price"] = round(random.uniform(10.0, 1000.0), 2)
    
    if action_type in ["button_click"]:
        metadata["button_id"] = f"btn_{random.randint(1, 100)}"
        metadata["button_text"] = random.choice(["Buy Now", "Add to Cart", "View Details", "Share"])
        metadata["screen"] = random.choice(["home", "product", "cart", "checkout", "profile"])
    
    if action_type in ["search"]:
        metadata["query"] = random.choice(["laptop", "phone", "book", "shoes", "watch"])
        metadata["results_count"] = random.randint(0, 500)
    
    if action_type in ["checkout_start", "checkout_complete"]:
        metadata["cart_value"] = round(random.uniform(50.0, 500.0), 2)
        metadata["items_count"] = random.randint(1, 10)
        metadata["payment_method"] = random.choice(["credit_card", "debit_card", "paypal", "apple_pay"])
    
    if action_type in ["location_search", "nearby_search"]:
        metadata["latitude"] = round(random.uniform(-90.0, 90.0), 6)
        metadata["longitude"] = round(random.uniform(-180.0, 180.0), 6)
        metadata["radius_km"] = random.randint(1, 50)
    
    if action_type in ["screen_view", "screen_exit"]:
        metadata["screen_name"] = random.choice(["home", "product_list", "product_detail", "cart", "checkout", "profile", "settings"])
        metadata["duration_seconds"] = random.randint(1, 300)
    
    # Add device info
    device_info = {
        "device_id": str(uuid.uuid4()),
        "device_type": random.choice(["ios", "android"]),
        "os_version": random.choice(["15.0", "16.0", "17.0", "12.0", "13.0", "14.0"]),
        "app_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
    }
    
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "action_type": action_type,
        "timestamp": timestamp.isoformat(),
        "metadata": metadata,
        "device_info": device_info
    }


def send_user_actions():
    producer = create_producer()
    topic_name = 'user_actions_topic'
    
    print(f"Starting to send user action events to topic: {topic_name}")
    
    # Send events continuously to simulate real-time stream
    # Each user performs ~50 actions per day, so we need high throughput
    total_events = 0
    events_per_second = 100  # Adjust based on requirements
    
    try:
        while True:
            for _ in range(events_per_second):
                event = generate_user_action()
                producer.send(topic_name, value=event)
                total_events += 1
                
                if total_events % 1000 == 0:
                    print(f"Sent {total_events} events...")
            
            time.sleep(1)  # Wait 1 second before next batch
            
    except KeyboardInterrupt:
        print(f"\nStopping producer. Total events sent: {total_events}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    from create_topic import create_topic
    
    # Create topic for user actions
    create_topic()
    
    # Wait a bit for Kafka to be ready
    time.sleep(5)
    
    # Start sending events
    send_user_actions()
