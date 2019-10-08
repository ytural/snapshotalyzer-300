import boto3
import botocore
import click

session = boto3.Session(profile_name='snapshotalyzer')
ec2 = session.resource('ec2')

def filter_instances(project):
    instances = []
    if project:
        filters = [{'Name': 'tag:Project', 'Values': [project]}]
        instances = ec2.instances.filter(Filters=filters)
    else:
        instances = ec2.instances.all()
    return instances


@click.group()
def cli():
    """Shotty manages snapshots"""


@cli.group('snapshots')
def snapshots():
    """Commands for snapshots"""


@snapshots.command('list')
@click.option('--project', default=None,
              help="Only snapshot for project (tag Project:<name>)")
def list_snapshots(project):
    """List EC2 snapshots"""
    instances = filter_instances(project)
    for i in instances:
        for v in i.volumes.all():
            for s in v.snapshots.all():
                print(', '.join((
                    s.id,
                    v.id,
                    i.id,
                    s.state,
                    s.progress,
                    s.start_time.strftime("%c")
                )))
    return


@cli.group('volumes')
def volumes():
    """Commands for volumes"""


@volumes.command('list')
@click.option('--project', default=None,
              help="Only volumes for project (tag Project:<name>)")
def list_volumes(project):
    """List EC2 volumes"""
    instances = filter_instances(project)
    for i in instances:
        for v in i.volumes.all():
            print(', '.join((
                v.id,
                i.id,
                str(v.iops) + "IOPS",
                str(v.volume_id),
                v.state,
                str(v.size) + "GIB",
                v.encrypted and "Encrypted" or "NotEncrypted"
            )))
    return


@cli.group('instances')
def instances():
    """Commands for instances"""


@instances.command('list')
@click.option('--project', default=None,
              help="Only instances for project (tag Project:<name>)")
def list_instances(project):
    """List EC2 instances"""

    instances = filter_instances(project)

    for i in instances:
        tags = {t['Key']: t['Value'] for t in i.tags or []}
        print(', '.join((
            i.id,
            i.instance_type,
            i.public_dns_name,
            i.state['Name'],
            i.placement['AvailabilityZone'],
            tags.get('Project', '<no project>')
        )))


@instances.command('stop')
@click.option('--project', default=None,
              help="Only instances for project (tag Project:<name>)")
def stop_instance(project):
    """Stop EC2 instances"""
    instances = filter_instances(project)
    for i in instances:
        print("Stopping {0}...".format(i.id))
        try:
            i.stop()
        except botocore.exceptions.ClientError as e:
            print(" Could not stop {0}. ").format(i.id)
            continue


@instances.command('start')
@click.option('--project', default=None,
              help="Only instances for project (tag Project:<name>)")
def start_instance(project):
    """Start EC2 instances"""
    instances = filter_instances(project)
    for i in instances:
        print("Starting {0}...".format(i.id))
        try:
            i.start()
        except botocore.exceptions.ClientError as e:
            print(" Could not start {0}. ").format(i.id)
            continue
    return


@instances.command('snapshot')
@click.option('--project', default=None,
              help="Only snapshot for project (tag Project:<name>)")
def create_snapshot(project):
    """Create EC2 snapshots"""
    instances = filter_instances(project)
    for i in instances:

        print("Stopping {0}...".format(i.id))
        i.stop()
        i.wait_until_stopped()

        for v in i.volumes.all():
            print("Creating snapshot of {0}".format(v.id))
            v.create_snapshot(Description='Created by SnapshotAlyzer 3000')

        print("Starting {0}...".format(i.id))
        i.start()
        i.wait_until_running()
    print("Job's done!")
    return


if __name__ == '__main__':
    cli()