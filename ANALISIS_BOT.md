# Análisis técnico del bot HFT (v18)

Fecha del análisis: 2026-05-20 (UTC)

## Resumen ejecutivo

El bot tiene una arquitectura avanzada (multi-hilo, market making por capas, kill switch por equity y simulación realista), pero presenta **riesgos operativos y de mantenimiento** que pueden provocar pérdidas silenciosas, incoherencias de estado o bloqueos difíciles de depurar.

Riesgo global estimado:
- **Alto** en producción live sin observabilidad adicional.
- **Medio** en simulación.

---

## Fortalezas detectadas

1. **Control de riesgo por equity real** y drawdown, mejor que un stop por PnL cerrado.
2. **Rate limiters explícitos** para órdenes y batch.
3. **Bloqueos (`mutex`) en datos compartidos críticos**, especialmente `Stats`.
4. **Dead Man's Switch** renovado periódicamente para cancelación remota.

---

## Posibles errores y puntos críticos

## 1) Excepciones silenciadas (`catch(...) {}`) en rutas críticas

Se usan múltiples `catch(...)` vacíos en cancelaciones, cierres, reconexión, lectura de balance, y pings WS. Esto puede ocultar fallos de red/API y dejar el estado interno divergiendo del exchange.

**Impacto:** alto. Puede aparentar que se canceló/cerró cuando no pasó.

**Recomendación:**
- Nunca dejar `catch(...)` vacío en operaciones de trading.
- Loguear siempre contexto mínimo (símbolo, oid, endpoint, excepción).

---

## 2) Uso intensivo de `std::thread(...).detach()`

Se lanzan hilos detached para batch/cancel/market orders.

**Riesgo principal:**
- Dificulta coordinar apagado limpio.
- Si hay tormenta de eventos, puede crecer la concurrencia y latencia.
- `rest_threads_` ayuda como contador, pero no evita backlog o starvation.

**Recomendación:**
- Migrar a pool fijo de workers + cola concurrente.
- Mantener límites duros de trabajos en cola por tipo de operación.

---

## 3) Cierre de inventario con PnL aparentemente incompleto

En `cerrar_inventory_`, el cálculo de `pnl_cierre` aplica solo para inventario largo (`q.inventory_usd>0`), mientras que para inventario corto no se ve una rama equivalente antes de acumular `q.pnl`.

**Impacto:** alto en métricas de rendimiento y decisiones de riesgo si los cortos son frecuentes.

**Recomendación:**
- Agregar rama explícita para short (`q.inventory_usd<0`).
- Añadir tests unitarios de PnL long/short con fees/slippage.

---

## 4) Dependencia fuerte de parsing manual de JSON vía `strstr`/`atof`

Ejemplo: extracción de `availableMargin` en respuesta REST con búsqueda de substring.

**Riesgos:**
- Frágil ante cambios menores del formato JSON.
- Posibles conversiones parciales/silenciosas.

**Recomendación:**
- Reemplazar por `boost::json` parseado estructurado.
- Validar claves requeridas y tipos numéricos.

---

## 5) Inconsistencia de `User-Agent` por endpoint/versionado

Se mezclan `HFT-MM/16.0` y `HFT-MM/11.0` en diferentes métodos REST.

**Impacto:** bajo-medio técnico, pero complica trazabilidad y debugging en logs externos.

**Recomendación:**
- Definir constante única de versión cliente.

---

## 6) Compilación no portable sin dependencias explícitas

El chequeo de sintaxis local falló por ausencia de Boost headers en el entorno.

**Impacto:** operativo (CI/CD/dev onboarding).

**Recomendación:**
- Añadir sección de dependencias concretas por distro.
- Proveer `Dockerfile` o script de bootstrap.

---

## 7) Riesgo de lock contention en `mq_`

`mq_` protege estado amplio de `quotes_` y se usa desde varios hilos; además algunos caminos de cancelación/cierre interactúan con operaciones de red asincrónicas.

**Impacto:** medio (latencia interna, jitter de requote).

**Recomendación:**
- Reducir sección crítica.
- Separar locks por símbolo o por subsistema (quotes/ordenes/metricas).

---

## 8) Observabilidad insuficiente para producción

Hay logging funcional, pero no métricas estructuradas para:
- tasa de error por endpoint,
- tiempo de roundtrip por operación,
- profundidad de cola de trabajos,
- razón de kill switch por clase.

**Recomendación:**
- Emitir métricas Prometheus/StatsD.
- Alarmas por umbrales (errores REST, reconexiones, inventario atascado).

---

## Priorización de correcciones

**P0 (inmediato):**
1. Eliminar `catch(...)` vacíos en rutas de órdenes/cancelaciones.
2. Corregir cálculo PnL de cierres short.
3. Validar parsing robusto de balance/equity con JSON real.

**P1 (próximo sprint):**
4. Sustituir `detach` por pool controlado.
5. Añadir tests de concurrencia y stress de rate limiter.

**P2 (hardening):**
6. Estandarizar telemetry y versionado de cliente REST.
7. Refinar granularidad de locks.

---

## Comandos ejecutados en este análisis

1. Inventario de archivos:
   - `rg --files`
2. Lectura de código/documentación:
   - `sed -n '1,220p' README.md`
   - `sed -n '1,260p' main_hmac_v18.cpp`
3. Búsqueda de patrones de riesgo:
   - `rg -n "TODO|FIXME|BUG|catch\(|throw|atomic|mutex|thread|while\(true\)|sleep_for|system\(" main_hmac_v18.cpp`
4. Chequeo sintáctico:
   - `g++ -std=c++17 -fsyntax-only -Wall -Wextra -Wpedantic main_hmac_v18.cpp -lboost_system -lssl -lcrypto -lpthread`

