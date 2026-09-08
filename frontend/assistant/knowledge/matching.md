# HR Insights ETL — Estrategia de Matching y Normalización

## El problema
Los mensajes del generador NO tienen un ID único global. Cada fragmento llega por separado (Personal, Location, Professional, Bank, Net) y hay que descubrir a qué persona pertenece.

## Claves de unión (por prioridad)
1. **Passport** — Personal y Bank comparten este campo. Es la clave más fiable. Formato: `passport:H85111106`
2. **Nombre completo normalizado** — Location y Professional tienen `Fullname`. Personal tiene `Name + Lastname` que se concatenan. Formato: `name:octavio ponce`
3. **Dirección** — Location y Net comparten este campo. Puente para fragmentos sin nombre ni passport. Formato: `addr:471 samantha cliff`

## Normalización de texto
Antes de comparar, todo pasa por:
- Lowercase
- Strip accents (NFKD decomposition): `García` → `garcia`
- Collapse whitespace
- Strip títulos/honoríficos: Mr, Mrs, Dr, Dr(a)., Ing., Lic., Mtro., Sr(a)., Dott., Sig., Prof.
- Strip sufijos: MD, PhD, Jr., Sr., II, III, Pi

Ejemplo: `"Dr(a). José  García MD"` → `"jose garcia"`

## Normalización de keys JSON
Las keys del generador son inconsistentes:
- `"E-Mail"` → `"email"`
- `"Company Address"` → `"companyaddress"`
- `"Company Adress"` (typo del generador) → `"companyaddress"`

## Cross-linking
Cuando un fragmento Personal tiene passport Y nombre, se registra un alias `nombre → passport` en Redis. Así, cuando llega el fragmento Location (que solo tiene nombre), se redirige al key de passport correcto y se unen correctamente.

## Reconciliación batch
Job periódico (Airflow) que detecta pares de registros probablemente duplicados:
- Registros con passport cuyo nombre es prefijo de otro registro por nombre
- Confianza = longitud_prefijo / longitud_total
- Nunca se auto-mergean, quedan como candidatos para revisión

## Limitación conocida
Si el generador produce un fullname con más palabras que name+lastname (ej: tercer apellido "Octavio Ponce Gimenez" vs "Octavio Ponce"), NO se cruzan. Es la decisión correcta para evitar falsos positivos.
