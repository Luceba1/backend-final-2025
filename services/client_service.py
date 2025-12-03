from sqlalchemy.orm import Session
from models.client import ClientModel
from repositories.client_repository import ClientRepository
from schemas.client_schema import ClientSchema
from services.base_service_impl import BaseServiceImpl


class ClientService(BaseServiceImpl):
    def __init__(self, db: Session):
        super().__init__(
            repository_class=ClientRepository,
            model=ClientModel,
            schema=ClientSchema,
            db=db
        )

    # OVERRIDE: este método es el que realmente usa el controlador
    def save(self, schema):
        # 1) Buscar cliente por email
        existing = self.repository.get_by_email(schema.email)

        if existing:
            # Si existe → usar ese y NO insertar
            return existing

        # 2) Si no existe → usar la lógica normal
        return super().save(schema)
