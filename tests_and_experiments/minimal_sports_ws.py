import asyncio
import websockets

WS_URL: str = "wss://sports-api.polymarket.com/ws"

async def main() -> None:
    async with websockets.connect(WS_URL) as ws:
        async for message in ws:
            print(message)

if __name__ == "__main__":
    asyncio.run(main())