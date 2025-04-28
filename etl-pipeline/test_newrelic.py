#!/usr/bin/env python
"""
Test script for New Relic custom events
"""
import os
import sys
import time
import newrelic.agent

def main():
    # Initialize the New Relic agent
    environment = os.environ.get('ENVIRONMENT', 'local')
    newrelic.agent.initialize('newrelic.ini', environment)
    
    # Register application explicitly with timeout
    print(f"Registering New Relic application for environment: {environment}")
    app = newrelic.agent.register_application(timeout=10.0)
    
    # Create a custom event with a timestamp
    event_type = "DevSmokeTest"
    event_data = {
        "message": "ping!",
        "timestamp": time.time(),
        "source": "test_script",
        "environment": environment,
    }
    
    # Use background task decorator to ensure proper monitoring
    @newrelic.agent.background_task(application=app, name="test_custom_event")
    def send_test_event():
        print(f"Sending custom event: {event_type}")
        newrelic.agent.record_custom_event(event_type, event_data, app)
        print("Event sent")
    
    # Send the event
    send_test_event()
    
    # Wait to ensure data is sent
    print("Waiting for event to be sent...")
    time.sleep(5)
    
    # Explicitly shutdown the agent
    print("Shutting down New Relic agent")
    newrelic.agent.shutdown_agent()
    print("Test complete")

if __name__ == "__main__":
    main() 