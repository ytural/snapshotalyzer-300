import boto3
import botocore
import click

session = boto3.Session(profile_name='snapshotalyzer')
ec2 = session.resource('ec2')


def filter_instances(project, instance):
    instances = []
    if project:
        filters = [{'Name': 'tag:Project', 'Values': [project]}]
        instances = ec2.instances.filter(Filters=filters)
    elif instance:
        filters = [{'Name':'instance-id', 'Values': [instance]}]
        instances = ec2.instances.filter(Filters=filters)
    else:
        instances = ec2.instances.all()
    return instances


def has_pending_snapshot(volume):
    snapshots = list(volume.snapshots.all())
    return snapshots and snapshots[0] == 'pending'


@click.group()
def cli():
    """Shotty manages snapshots"""


@cli.group('snapshots')
def snapshots():
    """Commands for snapshots"""


@snapshots.command('list')
@click.option('--project', default=None,
              help="Only snapshot for project (tag Project:<name>)")
@click.option('--all', 'list_all', default=False, is_flag=True,
              help="List all snapshots for each volume, not just the most recent")
def list_snapshots(project, list_all):
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
                if s.state == 'completed' and not list_all: break
    return


@cli.group('volumes')
def volumes():
    """Commands for volumes"""


@volumes.command('list')
@click.option('--project', default=None,
              help="Only volumes for project (tag Project:<name>)")
@click.option('--instance', default=None,
              help="Only volumes for instance ")
def list_volumes(project, instance):
    """List EC2 volumes"""
    instances = filter_instances(project, instance)
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
@click.option('--instance', default=None,
              help="Only specified instance ID ")
def list_instances(project, instance):
    """List EC2 instances"""

    instances = filter_instances(project, instance)

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
@click.option('--force', 'force_list', default=False, is_flag=True,
              help="Force to 'stop', 'start', 'snapshot', 'reboot' commands")
@click.option('--instance', default=None,
              help="Only specified instance ID ")
def stop_instance(project, force_list, instance):
    """Stop EC2 instances"""
    instances = filter_instances(project, instance)
    if project or force_list:
        for i in instances:
            print("Stopping {0}...".format(i.id))
            try:
                i.stop()
            except botocore.exceptions.ClientError as e:
                print(" Could not stop {0}. ").format(i.id)
                continue
    else:
        raise Exception("Please, run command either with --force or --project option")


@instances.command('start')
@click.option('--project', default=None,
              help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_list', default=False, is_flag=True,
              help="Force to 'stop', 'start', 'snapshot', 'reboot' commands")
@click.option('--instance', default=None,
              help="Only specified instance ID ")
def start_instance(project, force_list, instance):
    """Start EC2 instances"""
    instances = filter_instances(project, instance)
    if project or force_list:
        for i in instances:
            print("Starting {0}...".format(i.id))
            try:
                i.start()
            except botocore.exceptions.ClientError as e:
                print(" Could not start {0}. ").format(i.id)
                continue
        return
    else:
        raise Exception("Please, run command either with --force or --project option")


@instances.command('reboot')
@click.option('--project', default=None,
              help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force_list', default=False, is_flag=True,
              help="Force to 'stop', 'start', 'snapshot', 'reboot' commands")
@click.option('--instance', default=None,
              help="Only specified instance ID ")
def reboot_instance(project, force_list, instance):
    """Reboot EC2 instances"""
    instances = filter_instances(project, instance)
    if project or force_list:
        for i in instances:
            print("Rebooting {0}...".format(i.id))
            try:
                i.reboot()
            except botocore.exceptions.ClientError as e:
                print(" Could not reboot {0}. ").format(i.id)
                continue
        return
    else:
        raise Exception("Please, run command either with --force or --project option")


@instances.command('snapshot')
@click.option('--project', default=None,
              help="Only snapshot for project (tag Project:<name>)")
@click.option('--force', 'force_list', default=False, is_flag=True,
              help="Force to 'stop', 'start', 'snapshot', 'reboot' commands")
@click.option('--instance', default=None,
              help="Only specified instance ID ")
def create_snapshot(project, force_list, instance):
    """Create EC2 snapshots"""
    instances = filter_instances(project, instance)
    if project or force_list:
        for i in instances:
            try:
                print("Stopping {0}...".format(i.id))
                i.stop()
                i.wait_until_stopped()
                for v in i.volumes.all():
                    if has_pending_snapshot(v):
                        print(" Skipping {0}, snapshot already in progress".format(v.id))
                        continue
                    print("Creating snapshot of {0}".format(v.id))
                    v.create_snapshot(Description='Created by SnapshotAlyzer 3000')

                    print("Starting {0}...".format(i.id))
                    i.start()
                    i.wait_until_running()
                print("Job's done!")
            except botocore.exceptions.ClientError as e:
                print(" Could not reboot {0}. ").format(i.id)
                continue
    else:
        raise Exception("Please, run command either with --force or --project option")
    return


if __name__ == '__main__':
    cli()