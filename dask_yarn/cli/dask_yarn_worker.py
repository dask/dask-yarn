import click

@click.command()
@click.option('--memory-limit')
@click.option('--nthreads', type=int, default=0)
def main(memory_limit, nthreads):
    print('hello from worker', memory_limit, nthreads)

if __name__ == '__main__':
    main()
