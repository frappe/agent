def execute():
    """ add a new fc_agent_job_id field to JobModel """
    from agent.job import agent_database as database
    from playhouse.migrate import SqliteMigrator, migrate
    from peewee import CharField

    migrator = SqliteMigrator(database)
    try:

        migrate(
            migrator.add_column('JobModel', 'agent_job_id', CharField(null=True))
        )
    except Exception as e:
        print(e)
    