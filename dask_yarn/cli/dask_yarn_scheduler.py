import click
import skein


@click.command()
def main():
    print('hello from scheduler')
    client = skein.ApplicationClient.connect_to_current()
    loop = tornado.ioloop.IOLoop.current()
    scheduler = dask.distributed.Scheduler(loop=loop)
    scheduler.start()
    print(scheduler.address)
    client.kv['scheduler-address'] = scheduler.address
    loop.start()


if __name__ == '__main__':
    main()
