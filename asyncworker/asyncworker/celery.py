from celery import Celery

app = Celery(
    "asyncworker",
    broker="redis://localhost",
    backend="redis://localhost"
)
app.autodiscover_tasks(["asyncworker.tasks"])
