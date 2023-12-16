import asyncio
from collections import defaultdict


async def command_analyzer(commands_queue, stats_dict):
    while True:
        command = await commands_queue.get()
        match command:
            case ["SET", *_]:
                stats_dict["set_count"] += 1
            case ["DEL", *_]:
                stats_dict["del_count"] += 1
            case _:
                stats_dict["unknown_count"] += 1
        print(stats_dict)


async def line_aggregator(lines_queue, commands_queue):
    lines = []
    count = 0
    while True:
        line = await lines_queue.get()
        
        if line.startswith("*") and len(lines) == count:
            lines.clear()
            count = int(line.split("*")[1])
        elif not line.startswith("$"):
            lines.append(line)

        if len(lines) == count:
            await commands_queue.put(lines)


async def writer_function(reader, client_writer, lines_queue):
    while True:
        request = await reader.readuntil()
        await lines_queue.put(request.decode().strip())
        client_writer.write(request)
        await client_writer.drain()


async def reader_function(writer, client_reader):
    while True:
        response = await client_reader.readuntil()
        writer.write(response)
        await writer.drain()


async def handle_echo(reader, writer, stats_dict):
    client_reader, client_writer = await asyncio.open_connection(
        '192.168.64.2', 6379)

    lines_queue = asyncio.Queue()
    commands_queue = asyncio.Queue()

    periodic_a_task = asyncio.create_task(writer_function(reader, client_writer, lines_queue))
    periodic_b_task = asyncio.create_task(reader_function(writer, client_reader))
    periodic_c_task = asyncio.create_task(line_aggregator(lines_queue, commands_queue))
    periodic_d_task = asyncio.create_task(command_analyzer(commands_queue, stats_dict))

    await asyncio.gather(periodic_a_task, periodic_b_task, periodic_c_task, periodic_d_task)


async def main():
    stats_dict = defaultdict(int)
    server = await asyncio.start_server(
        lambda r, w: handle_echo(r, w, stats_dict), '0.0.0.0', 8888)

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()


asyncio.run(main())
