---
PLAN: "test: correr la misma suite contra mem y contra indexdb"
TAG: v0.2.0
EXECUTOR: unassigned
REVIEWER: none
STATUS: running
SESSION: 15163692461147076599
---

> Iteración sobre `webtyp/vectordb` v0.1.0. Índice maestro:
> https://github.com/webtyp/agent/blob/main/docs/PLAN.md
>
> **Nota de idioma:** la prosa va en español; los bloques de código mantienen sus
> comentarios en inglés, como el resto del código fuente de este repositorio.

# Plan — la suite corre dos veces, no una

## Por qué existe este plan

`vectordb/docs/PLAN.md` (v0.1.0, ya publicado) pedía explícitamente: *"La suite corre dos
veces: contra `storage/mem` en Go estándar, y contra `webtyp.com/indexdb` en un navegador
bajo `gotest -tinygo`. Mismos cuerpos de test, distinta factory — el patrón que
`indexdb/tests/conformance_test.go` ya usa."*

Esa parte **no se implementó**. `vectordb_test.go` importa únicamente `webtyp.com/storage/mem`
— no hay un solo archivo con `//go:build wasm`, y `webtyp.com/indexdb` no aparece en ningún
lado del repositorio, ni siquiera en tests.

Esto no es un detalle menor: es el mecanismo que existe específicamente para atrapar bugs que
`mem` no puede exponer. Y atrapó uno real, ya arreglado en v0.1.0 por otra vía (revisión
manual, no por este mecanismo): `Add()` llamaba a `evictIfNeeded(exec)` **después** de
`tx.Commit()`, usando el executor de una transacción ya cerrada. Contra `mem` eso no falla
—`mem` no invalida su executor al comitear—. Contra `indexdb` real, una transacción de
IndexedDB comiteada es inutilizable; el mismo código habría fallado ahí. El bug estuvo
invisible precisamente porque nunca corrió contra el backend real.

**Este plan no busca bugs nuevos a propósito.** Busca que la infraestructura que los atrapa
exista, para que el próximo bug de esta clase no dependa de que alguien lo encuentre leyendo
código a mano.

## El obstáculo que hay que resolver primero, explícitamente

`indexdb.New(dbName, idg, logger, structTables ...any)` necesita instancias que implementen
`model.Model` para declarar los object stores **antes** de que `vectordb.New()` corra —
`storage.Conn` es puramente DML (`storage/AGENTS.md`), así que la creación de tablas es
responsabilidad de quien arma el backend, no de `vectordb`.

El problema: los tipos que implementan `model.Model` para `vec_docs`/`vec_shards`/`vec_index`
—`docRecord`, `shardRecord`, `indexRecord`— son **privados** (`models.go`). Un test externo
(`package vectordb_test`) no puede construirlos, y por lo tanto no puede armar el
`indexdb.New(...)` que necesita para levantar el backend de prueba.

Esto no es solo un problema de tests: es un hueco real de la API. **Cualquier aplicación**
que quiera usar `vectordb` sobre `indexdb` en producción se topa con el mismo problema —no
hay forma de decirle a `indexdb.New` qué object stores declarar sin poder nombrar los tipos.

**La solución es exportar una función mínima, no debilitar el aislamiento del test:**

```go
// Schema returns the model.Model values a DDL-capable backend (indexdb.New,
// ddl.CreateTable) needs to declare vectordb's three tables/object stores
// before New is called. storage.Conn is DML-only — table creation is always
// the caller's responsibility, and this is what a caller names to do it.
func Schema() []model.Model {
	return []model.Model{&docRecord{}, &shardRecord{}, &indexRecord{}}
}
```

En `schema.go`, junto a `DocModel`/`ShardModel`/`IndexModel`. Gate del skill de diseño de
API, resuelto acá para no repetirlo en el PR:

1. **Arte previo:** es exactamente lo que `indexdb/tests/conformance_test.go` ya consume de
   `storage/conformance` — una lista de `model.Model` para declarar antes de `New`.
2. **Test del novato:** `vectordb.Schema()` — "el esquema de vectordb". Sin ambigüedad.
3. **Libro mayor:** `+1` símbolo exportado, `-0` en todo lo demás — esto no reemplaza nada,
   es lo que faltaba para que un backend con DDL sea usable desde afuera del paquete.
4. **Dónde pertenece:** en `vectordb`, porque es dueño del esquema (`schema.go` ya lo es).
5. **Qué borra:** nada — es la primera vez que existe una forma de hacer esto.

## Restructuración de `vectordb_test.go`

**No reescribas los asserts.** Los 16 tests ya verifican lo correcto contra `mem`; el trabajo
es parametrizar CÓMO consiguen su `storage.Conn`, no QUÉ verifican.

1. Cada `func TestX(t *testing.T) { ... }` pasa a `func testX(t *testing.T, newConn func(t
   *testing.T) storage.Conn) { ... }`, reemplazando cada `mem.New()` interno por
   `newConn(t)`. El resto del cuerpo —construcción de `Config`, llamadas a `Add`/`Search`/
   `Delete`, los `t.Fatalf`/`t.Errorf`— **no cambia una línea**.
2. Los tipos ya reutilizables entre backends —`mockIDGen`, `fixedVectorEmbedder`,
   `txRecorderConn`, `txRecorderBound`— quedan donde están, no se tocan.
3. Archivo nuevo, **sin build tag**, `package vectordb_test`: `vectordb_mem_test.go`.
   Dieciséis funciones `TestX(t *testing.T) { testX(t, memConn) }` donde:
   ```go
   func memConn(t *testing.T) storage.Conn { return mem.New() }
   ```
4. Archivo nuevo, `//go:build wasm`, `package vectordb_test`: `vectordb_indexdb_test.go`.
   Mismas dieciséis funciones, mismo patrón, contra un factory construido con
   `vectordb.Schema()` y `webtyp.com/indexdb`, siguiendo el patrón exacto de
   `indexdb/tests/conformance_test.go` — un nombre de base de datos distinto por test, para
   que no haya sangrado entre tests:
   ```go
   func indexdbConn(t *testing.T) storage.Conn {
       return indexdb.New(freshDBName(t), &mockIDGen{}, nil, toAny(vectordb.Schema())...)
   }
   ```
   (`freshDBName` y `toAny` son helpers chicos que escribís vos; no hace falta que existan
   en ningún otro repositorio.)
5. **`vectordb_test.go` original queda vacío de tests y se borra**, o se convierte en el
   archivo que solo declara los tipos compartidos (`mockIDGen`, etc.) si preferís no
   duplicarlos en los dos drivers. Vos decidís la organización de archivos; lo que no es
   negociable es que los 16 cuerpos de test sean **una sola función cada uno**, llamada
   desde ambos drivers.

## Puerta de aceptación

```bash
go vet ./...
gotest                  # backend mem, Go estándar — los mismos 16 tests que ya pasaban
gotest -tinygo          # backend indexdb, navegador real — los MISMOS 16 tests
grep -rn "webtyp.com/indexdb" --include="*.go" . | grep -v _test.go   # → vacío
```

**No alcanza con que compile.** Los 16 tests tienen que pasar contra `indexdb` en un
navegador real, no solo bajo `GOOS=js GOARCH=wasm go build`. Si alguno falla contra `indexdb`
y pasaba contra `mem`, esa diferencia es información — repórtala, no la escondas ajustando el
test para que pase en los dos backends por igual si el comportamiento correcto es distinto.

Prestá atención en particular a `TestEvict_LeastUsedOldestFirst` y `TestEvict_CompactsShards`
—los que ejercitan escritura + transacción + desalojo juntos— porque son los que habrían
atrapado el bug de `Add()`/`evictIfNeeded` que este plan documenta como motivación. Confirmá
que pasan contra `indexdb`, no solo que compilan.

## Lo que este plan NO hace

No toca `add_delete.go`, `search.go`, `store.go` ni la lógica de negocio — v0.1.0 ya está
corregido y publicado. Esto es exclusivamente infraestructura de test.
