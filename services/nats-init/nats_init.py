import asyncio
import os
import nats


async def create_streams():
    nats_url = os.environ.get("NATS_URL", "nats://nats:4222")
    nc = await nats.connect(nats_url)

    try:
        print(f"Connected to NATS at {nats_url}")

        js = nc.jetstream()

        # Create EVENTS stream
        try:
            await js.add_stream(name="EVENTS", subjects=["EVENTS.*"])
            print("EVENTS stream created successfully")
        except Exception as e:
            print(f"Error creating EVENTS stream: {e}")

        # Create COMMANDS stream
        try:
            await js.add_stream(name="COMMANDS", subjects=["COMMANDS.*"])
            print("COMMANDS stream created successfully")
        except Exception as e:
            print(f"Error creating COMMANDS stream: {e}")

    except Exception as e:
        print(f"Error connecting to NATS: {e}")
    finally:
        await nc.close()


async def main():
    await create_streams()

    # Keep the service running to satisfy Docker Compose
    while True:
        await asyncio.sleep(3600)  # Sleep for an hour


if __name__ == "__main__":
    asyncio.run(main())
