# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

from uuid import uuid4

from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.future import Engine

from app.services.approval.models import ApprovalEntities


class ApprovalServiceClient:
    """Get information about approval request or entities for copy request."""

    def __init__(self, engine: Engine, metadata: MetaData) -> None:
        self.engine = engine
        self.metadata = metadata

        self.approval_entity = Table(
            'approval_entity',
            self.metadata,
            Column('id', UUID(as_uuid=True), unique=True, primary_key=True, default=uuid4),
            keep_existing=True,
            autoload_with=self.engine,
        )

    def get_approval_entities(self, request_id: str) -> ApprovalEntities:
        """Return all approval entities related to request id."""

        statement = select(self.approval_entity).filter_by(request_id=request_id)
        cursor = self.engine.connect().execute(statement)

        request_approval_entities = ApprovalEntities.from_cursor(cursor)

        return request_approval_entities
