"""Initial jobs table

Revision ID: 001_initial_jobs
Revises: 
Create Date: 2026-10-27 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial_jobs'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'jobs',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('task_name', sa.String(length=255), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default='{}'),
        sa.Column('status', sa.String(length=32), nullable=False, server_default='PENDING'),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('max_attempts', sa.Integer(), nullable=False, server_default='3'),
        sa.Column('result', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('worker_id', sa.String(length=255), nullable=True),
        sa.Column('locked_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_jobs_task_name', 'jobs', ['task_name'])
    op.create_index('ix_jobs_status', 'jobs', ['status'])
    op.create_index('ix_jobs_created_at', 'jobs', ['created_at'])
    op.create_index('ix_jobs_status_locked_at', 'jobs', ['status', 'locked_at'])


def downgrade() -> None:
    op.drop_index('ix_jobs_status_locked_at', table_name='jobs')
    op.drop_index('ix_jobs_created_at', table_name='jobs')
    op.drop_index('ix_jobs_status', table_name='jobs')
    op.drop_index('ix_jobs_task_name', table_name='jobs')
    op.drop_table('jobs')
