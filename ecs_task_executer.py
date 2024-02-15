from aws_services.aws_ecs import ECS


def run_ecs_task():
    task_definition = 'recruitment-crawler-prod'
    container_name = 'recruitment-crawler-prod'
    environment_variables = []

    ECS().run_task(
        task_definition=task_definition,
        container_name=container_name,
        environment_variables=environment_variables
    )


if __name__ == '__main__':
    run_ecs_task()
