# Tech Debt

## webhook_url validation
Заменить regex `^https?://` на `Pydantic AnyHttpUrl` — полноценная валидация URL.
- Файл: `app/schemas/payment.py`

## verify_api_key на уровне роутера
Перенести `dependencies=[Depends(verify_api_key)]` с отдельных endpoint'ов на уровень `APIRouter` — уменьшит дублирование при росте количества эндпоинтов.
- Файл: `app/api/v1/payments.py`, `app/api/v1/router.py`
