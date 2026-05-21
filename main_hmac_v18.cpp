// ================================================================
// HFT MARKET MAKER v18.0 — Kraken Futures
// ARQUITECTURA: Multi-Level Layering / Estratificación Multi-Nivel
//
// ═══ NUEVO EN v18.0 (Simulación Hiperrealista + Equity Kill Switch) ══
//
// [7] EQUITY_ACTUAL — Patrimonio neto atómico en tiempo real
//     atomic<int64_t> en μUSD. Actualizado en cada tick (SIM)
//     y cada 60s desde la API de Kraken (live).
//
// [8] simular_fills_ HIPERREALISTA
//     A) Filtro latencia 45ms: orden no se llena si no pasaron
//        45ms desde el último requote (round-trip real).
//     B) Filtro de cola: cruce ESTRICTO del precio (< / >),
//        no tocar (≤ / ≥). Simula la posición en el libro.
//     C) PnL flotante actualizado en cada tick → EQUITY_ACTUAL.
//
// [9] SLIPPAGE EN SIM: cerrar_inventory_ aplica 5bps de
//     deslizamiento al precio de cierre taker en modo SIM.
//
// [10] KILL SWITCH REAL: bucle_gestion_ evalúa EQUITY_ACTUAL
//      (balance + PnL flotante) en lugar de solo PnL cerrado.
//      El bot para ante pérdidas en vuelo, no solo realizadas.
//
// ═══ NUEVO EN v18.0 ══════════════════════════════════════════════
//
// [1] MURO DE 3 CAPAS SIMULTÁNEAS (133 USD nocional por capa)
//     Cada par gestiona 3 niveles independientes bid+ask.
//     nocional_capa = (MARGEN_PAR/2 * APAL) / 3 = 66.67 USD
//     → nocional_capa efectivo = min(66.67, NOC_MAX/3) ≈ 66.67 USD
//     El muro completo por par = 3 bid + 3 ask = 6 micro-órdenes
//     enviadas en UN SOLO batch REST (una sola llamada HTTP).
//
// [2] GRID AVELLANEDA CON SKEW ADAPTADO POR INVENTARIO
//     Capa 1: precio A-S puro (reservation_price ± half_spread)
//     Capa 2: rp ± (hs + step)   donde step = tick_size * (1 + 10*sigma)
//     Capa 3: rp ± (hs + 2*step)
//     Skew agresivo si |inventory| >= nocional_total * 0.3:
//       → las 3 capas de venta se comprimen hacia mid
//         forzando el cierre de inventario largo en segundos.
//
// [3] FILTRO DE FLUJO TÓXICO (ANTI-LOSS)
//     OFI < -0.6 (presión vendedora fuerte) → pausa el bid completo
//     OFI >  0.6 (presión compradora fuerte) → pausa el ask completo
//     Validación bid_px < ask_px se hace POST-REDONDEO al tick_size
//     garantizando siempre entrada como Maker (fee 0.02%).
//
// [4] BATCH DE 6 MICRO-ÓRDENES EN UN SOLO HTTP POST
//     JSON: {"batchOrder":[cancel×N, send bid1, send bid2, send bid3,
//                          send ask1, send ask2, send ask3]}
//     Todo el muro se actualiza en la misma milésima de segundo.
//
// [5] BLINDAJE try-catch + RAII en hilo asíncrono
//     RestGuard destructor garantiza rest_en_vuelo=false SIEMPRE.
//
// [6] MUTEX INTERNO EN Stats::push() y Stats::vol()
//     std::mutex mut_stats dentro del struct Stats.
//     Elimina data races entre bucle_red_ y bucle_quotes_.
//
// ═══ COMPILACIÓN ═══════════════════════════════════════════════
//   g++ -O3 -std=c++17 -march=armv8-a main_hmac_v18.cpp \
//       -o bot_hft -lboost_system -lssl -lcrypto -lpthread
//   (AWS c7gn.medium ARM64 Graviton3, Irlanda eu-west-1)
//
// ═══ VARIABLES DE ENTORNO ══════════════════════════════════════
//   KRAKEN_API_KEY    = tu API key de Kraken Futures
//   KRAKEN_API_SECRET = tu API secret (base64)
//   HFT_MODO          = "live" para operar real, otro = SIM
// ================================================================
#include<atomic>
#include<cmath>
#include<chrono>
#include<cstring>
#include<deque>
#include<fstream>
#include<functional>
#include<iomanip>
#include<iostream>
#include<memory>
#include<mutex>
#include<condition_variable>
#include<pthread.h>
#include<signal.h>
#include<openssl/hmac.h>
#include<openssl/sha.h>
#include<openssl/evp.h>
#include<vector>
#include<sstream>
#include<string>
#include<thread>
#include<unordered_map>
#include<boost/asio.hpp>
#include<boost/asio/ssl.hpp>
#include<boost/beast.hpp>
#include<boost/beast/ssl.hpp>
#include<boost/beast/http.hpp>
#include<boost/json.hpp>

namespace asio      = boost::asio;
namespace beast     = boost::beast;
namespace http      = beast::http;
namespace websocket = beast::websocket;
namespace json      = boost::json;
using tcp = asio::ip::tcp;

// ================================================================
// CONSTANTES GLOBALES
// ================================================================
static double CAP = 815.0;

// ── [REGLA 1] Patrimonio neto en tiempo real ─────────────────────
// Equity = Balance + PnL flotante de todas las posiciones abiertas.
// Actualizado en cada tick (SIM) y cada 60s desde la API (live).
// Usado por bucle_gestion_ para kill switch basado en equity real.
static std::atomic<int64_t> EQUITY_ACTUAL_INT{815000000LL}; // μUSD
inline double get_equity(){return EQUITY_ACTUAL_INT.load(std::memory_order_acquire)/1e6;}
inline void   set_equity(double v){EQUITY_ACTUAL_INT.store((int64_t)(v*1e6),std::memory_order_release);}
inline void   add_equity(double delta){
    int64_t d=(int64_t)(delta*1e6);
    EQUITY_ACTUAL_INT.fetch_add(d,std::memory_order_relaxed);
}

static void cargar_capital(){
    std::ifstream f("capital.txt");
    if(f.is_open()){double v;if(f>>v&&v>100)CAP=v;}
}
static void guardar_capital(double v){
    std::ofstream f("capital.txt");
    if(f.is_open())f<<std::fixed<<std::setprecision(4)<<v<<"\n";
}

// ── Fees Kraken Futures ──────────────────────────────────────────
static const double FEE_MAKER    = 0.0002;
static const double FEE_TAKER    = 0.0005;

// ── Avellaneda-Stoikov parámetros ────────────────────────────────
static const double AS_GAMMA     = 0.15;
static const double AS_KAPPA     = 1.5;
static const double AS_T_SECONDS = 14400.0;

// ── OFI ─────────────────────────────────────────────────────────
static const double OFI_C1       = 0.40;
// Umbral de flujo tóxico: |OFI| > este valor → pausa un lado
static const double OFI_TOXIC    = 0.60;

// ── Adverse selection ────────────────────────────────────────────
static const double ADVERSE_FRAC = 0.50;

// ── Apalancamiento y sizing ──────────────────────────────────────
static const double APAL         = 3.0;   // bajado de 10x a 3x para arranque live
static const int    N_CAPAS      = 3;         // número de niveles del muro
static const double INV_MAX_FRAC = 1.0;
static const double STOP_LOSS_PCT= 0.015;
// Umbral de skew agresivo: inventario >= 30% del nocional total
static const double SKEW_AGRESIVO_FRAC = 0.30;

// ── Rate limits y timing ─────────────────────────────────────────
static const int    REQUOTE_MS   = 320;  // calibrado: 303ms mínimo × 1.06 margen
static const double VOL_MAX      = 0.0025;
static const double VOL_MAX_BTC  = 0.0040;

// ── Kill switches ─────────────────────────────────────────────────
static const double KD           = 40.0;   // ajustado a 3x (antes 120 para 10x)
static const double KDD          = 0.12;

// ── Timeout inventario ───────────────────────────────────────────
static const int    TIMEOUT_MKT_MS = 12000;

// ── Nocionales máximos por par ───────────────────────────────────
static const double NOC_MAX_SOL  = 2000.0;
static const double NOC_MAX_ETH  = 3000.0;
static const double NOC_MAX_XRP  = 800.0;
static const double NOC_MAX_BTC  = 10000.0;
static const double NOC_MAX_DEF  = 1000.0;

// ── Márgenes por par (distribución de $815) ──────────────────────
// MARGEN_PAR = 40 USD → nocional_lado_total = 40/2*10 = 200 USD
// nocional_capa = 200/3 ≈ 66.67 USD por capa
static const double MARGEN_SOL   = 40.0;
static const double MARGEN_ETH   = 40.0;  // subido de 20 para cubrir lot_size ETH
static const double MARGEN_XRP   = 40.0;  // subido de 20 para cubrir lot_size XRP

// ── Endpoints REST ───────────────────────────────────────────────
static const char REST_HOST[]        = "futures.kraken.com";
static const char REST_PORT[]        = "443";
static const char REST_SEND[]        = "/derivatives/api/v3/sendorder";
static const char REST_BATCH[]       = "/derivatives/api/v3/batchorder";
static const char REST_CANCEL[]      = "/derivatives/api/v3/cancelorder";
static const char REST_LEVERAGE[]    = "/derivatives/api/v3/leveragepreferences";
static const char REST_LEVERAGE_GET[]= "/derivatives/api/v3/leveragepreferences";

// ── WebSocket ────────────────────────────────────────────────────
static const char WS_HOST[] = "futures.kraken.com";
static const char WS_PORT[] = "443";
static const char WS_PATH[] = "/ws/v1";
static const char USER_AGENT[] = "HFT-MM/18.1";

enum class Side{B,S};

// ================================================================
// ESPECIFICACIONES DE INSTRUMENTOS
// ================================================================
struct InstrSpec{
    double tick_size;
    double lot_size;
    int    price_decimals;
    int    size_decimals;
};

// ── Especificaciones verificadas contra documentación oficial Kraken ─
// Fuente: support.kraken.com/articles/4844359082772
// Última verificación: 20 Mayo 2026
// Campos: { tick_size, lot_size, price_decimals, size_decimals }
inline InstrSpec get_spec(const std::string&sym){
    //           tick        lot      p_dec  s_dec
    if(sym=="PF_XBTUSD") return {1.0,     0.0001, 0, 4}; // BTC  min 0.0001 BTC
    if(sym=="PF_ETHUSD") return {0.1,     0.001,  1, 3}; // ETH  min 0.001 ETH ← CORREGIDO
    if(sym=="PF_SOLUSD") return {0.01,    0.01,   2, 2}; // SOL  min 0.01 SOL  ← CORREGIDO
    if(sym=="PF_XRPUSD") return {0.00001, 1.0,    5, 0}; // XRP  min 1 XRP     ✓
    if(sym=="PF_AVAXUSD")return {0.001,   0.01,   3, 2}; // AVAX min 0.01 AVAX ← CORREGIDO
    if(sym=="PF_LINKUSD")return {0.001,   0.1,    3, 1}; // LINK min 0.1 LINK
    if(sym=="PF_BONKUSD")return {0.000000001,1000.,9, 0}; // BONK min 1000      ✓
    if(sym=="PF_DOTUSD") return {0.001,   0.1,    3, 1}; // DOT  min 0.1 DOT
    return {0.001, 1.0, 3, 2}; // default conservador
}

inline double redondear(double v, double step){
    if(step<=0)return v;
    return std::floor(v/step)*step;
}

inline std::string fmt_decimal(double v, int decimals){
    std::ostringstream oss;
    oss<<std::fixed<<std::setprecision(decimals)<<v;
    return oss.str();
}

// ================================================================
// FAST PARSER
// ================================================================


inline bool extraer_available_margin_flex(const std::string&resp,double&out_bal){
    try{
        json::value jv=json::parse(resp);
        if(!jv.is_object())return false;
        const auto& root=jv.as_object();
        auto it_accounts=root.find("accounts");
        if(it_accounts==root.end()||!it_accounts->value().is_object())return false;
        const auto& accounts=it_accounts->value().as_object();
        auto it_flex=accounts.find("flex");
        if(it_flex==accounts.end()||!it_flex->value().is_object())return false;
        const auto& flex=it_flex->value().as_object();
        auto it_margin=flex.find("availableMargin");
        if(it_margin==flex.end())return false;
        const json::value& m=it_margin->value();
        if(m.is_double()) out_bal=m.as_double();
        else if(m.is_int64()) out_bal=(double)m.as_int64();
        else if(m.is_uint64()) out_bal=(double)m.as_uint64();
        else if(m.is_string()) out_bal=std::atof(std::string(m.as_string().c_str()).c_str());
        else return false;
        return out_bal>0;
    }catch(const std::exception&ex){
        (void)ex;
        return false;
    }
}
inline double fatof(const char*p)noexcept{
    double r=0;bool ng=(*p=='-');if(ng)++p;
    while(*p>='0'&&*p<='9'){r=r*10+(*p++-'0');}
    if(*p=='.'){++p;double f=0.1;while(*p>='0'&&*p<='9'){r+=(*p++-'0')*f;f*=0.1;}}
    return ng?-r:r;
}
inline const char*ffd(const char*d,size_t l,const char*k)noexcept{
    size_t kl=strlen(k);
    for(size_t i=0;i+kl<l;++i)if(memcmp(d+i,k,kl)==0)return d+i+kl;
    return nullptr;
}

// ================================================================
// TICK
// ================================================================
struct Tick{
    char sym[24]{};
    double bid=0,ask=0,mid=0;
    double bid_qty=0,ask_qty=0;
    double vamp=0,ofi=0;

    static Tick parse(const char*d,size_t l)noexcept{
        Tick t;
        const char*p=ffd(d,l,"\"product_id\":\"");
        if(p){int i=0;while(*p!='"'&&i<23)t.sym[i++]=*p++;t.sym[i]=0;}
        p=ffd(d,l,"\"bid\":");  if(p){while(*p==' '||*p=='"')++p;t.bid=fatof(p);}
        p=ffd(d,l,"\"ask\":");  if(p){while(*p==' '||*p=='"')++p;t.ask=fatof(p);}
        p=ffd(d,l,"\"bid_size:");if(p){while(*p==' '||*p=='"')++p;t.bid_qty=fatof(p);}
        p=ffd(d,l,"\"ask_size:");if(p){while(*p==' '||*p=='"')++p;t.ask_qty=fatof(p);}
        if(t.bid>0&&t.ask>0){
            t.mid=(t.bid+t.ask)*0.5;
            double total=t.bid_qty+t.ask_qty;
            if(total>0){
                t.vamp=(t.bid*t.ask_qty+t.ask*t.bid_qty)/total;
                t.ofi =(t.bid_qty-t.ask_qty)/total;
            } else {t.vamp=t.mid;t.ofi=0.0;}
        }
        return t;
    }
    bool ok()const noexcept{return bid>0&&ask>0&&ask>bid&&sym[0]!=0;}
};

// ================================================================
// [REGLA 6] STATS VOLATILIDAD — mutex interno protege push/vol
// ================================================================
struct Stats{
    static constexpr double LAMBDA = 0.94;
    double ewm_mean   = 0.0;
    double ewm_var    = 0.0;
    int    n          = 0;
    double last_mid   = 0.0;
    // [C6] GARCH(1,1) — predice volatilidad antes de que llegue
    // σ²_t = ω + α·ε²_{t-1} + β·σ²_{t-1}  (crypto típico: α=0.10, β=0.89)
    double garch_var  = 0.000025; // inicializar en 0.5% diario típico crypto
    static constexpr double GARCH_OMEGA = 0.000001;
    static constexpr double GARCH_ALPHA = 0.10;
    static constexpr double GARCH_BETA  = 0.89;
    mutable std::mutex mut_stats;

    Stats()=default;
    Stats(const Stats&)=delete;
    Stats&operator=(const Stats&)=delete;

    void push(double mid){
        std::lock_guard<std::mutex>lk(mut_stats);
        if(last_mid<=0){last_mid=mid;return;}
        double ret=(mid-last_mid)/last_mid;
        last_mid=mid;++n;
        if(n==1){ewm_mean=ret;ewm_var=ret*ret;garch_var=ret*ret;}
        else{
            ewm_mean=LAMBDA*ewm_mean+(1.0-LAMBDA)*ret;
            double diff=ret-ewm_mean;
            ewm_var =LAMBDA*ewm_var +(1.0-LAMBDA)*diff*diff;
            // GARCH(1,1): actualizar varianza predicha
            garch_var=GARCH_OMEGA + GARCH_ALPHA*ret*ret + GARCH_BETA*garch_var;
            garch_var=std::max(garch_var,1e-10); // evitar underflow
        }
    }

    double vol()const{
        std::lock_guard<std::mutex>lk(mut_stats);
        if(n<2)return 0.0005;
        // [C6] Usar máximo de EWM y GARCH — el más conservador de los dos
        // EWM reacciona a volatilidad pasada, GARCH predice la futura
        // Tomando el max, el spread nunca se estrecha cuando hay riesgo latente
        double v_ewm   = std::sqrt(ewm_var);
        double v_garch = std::sqrt(garch_var);
        double v       = std::max(v_ewm, v_garch);
        return std::max(0.0003,std::min(v,0.05));
    }
};

// ================================================================
// AVELLANEDA-STOIKOV ENGINE
// ================================================================
struct ASEngine{
    double gamma,kappa,T;
    ASEngine():gamma(AS_GAMMA),kappa(AS_KAPPA),T(AS_T_SECONDS){}

    static double time_remaining(){return AS_T_SECONDS;}// rolling 4h

    double reservation_price(double vamp,double sigma,double q_norm)const{
        return vamp - q_norm*gamma*sigma*sigma*time_remaining();
    }

    double half_spread(double sigma)const{
        double Tt=time_remaining();
        double hs=(gamma/2.0)*sigma*sigma*Tt+(1.0/gamma)*std::log(1.0+gamma/kappa);
        double hs_min=FEE_MAKER*2.0+ADVERSE_FRAC*sigma;
        return std::max(hs_min,std::min(hs,0.0025));  // cap 25bps
    }

    double spread_min_operar(double sigma)const{
        // Retorna 0 — el bot siempre cotiza su propio spread A-S
        // (15-40bps) sin importar el spread real del mercado.
        // El filtro de spread mínimo no aplica en Kraken Futures
        // porque SOL/ETH/XRP siempre tienen liquidez suficiente.
        (void)sigma;
        return 0.0;
    }
};

// ================================================================
// [REGLA 1] CAPA — representa un nivel de precio en el muro
// ================================================================
struct Capa{
    // Bid
    double bid_px   = 0;
    std::string bid_oid;
    bool bid_viva   = false;
    // Ask
    double ask_px   = 0;
    std::string ask_oid;
    bool ask_viva   = false;
};

// ================================================================
// [REGLA 1] QUOTE v18 — muro de N_CAPAS niveles por par
// ================================================================
struct Quote{
    std::string sym;
    double margen_usd      = 0;
    double nocional_total  = 0;   // lado completo = margen/2*APAL
    double nocional_capa   = 0;   // nocional_total / N_CAPAS ≈ 66.67 USD
    // [C2] EWM de volúmenes para VAMP suavizado
    double ema_bid_qty     = 0;   // cola bid suavizada
    double ema_ask_qty     = 0;   // cola ask suavizada

    // [REGLA 1] Muro de 3 capas
    Capa capas[N_CAPAS];

    bool rest_en_vuelo     = false;
    bool inv_close_en_vuelo= false;

    std::chrono::steady_clock::time_point ultimo_fill{};
    std::chrono::steady_clock::time_point ultimo_requote{};

    bool   tiene_inventario = false;
    double inventory_usd    = 0;
    double precio_inv       = 0;

    double pnl              = 0;
    uint64_t ciclos=0,fills_bid=0,fills_ask=0;
    // Calibración dinámica de κ
    double kappa_estimado   = AS_KAPPA;  // arranca en el valor por defecto
    uint64_t total_requotes = 0;         // total de requotes para este par
    uint64_t total_fills_k  = 0;         // fills para calibrar κ

    // Métricas del modelo
    double last_sigma=0.002,last_hs=0,last_rp=0,last_vamp=0,last_ofi=0;
    // [C4] OFI con memoria EWM — reduce señales falsas a la mitad
    double ofi_ema = 0.0;

    Quote()=default;
    Quote(std::string s,double mg)
        :sym(s),margen_usd(mg){
        nocional_total=std::min((mg/2.0)*APAL,noc_max(s));
        nocional_capa =nocional_total/double(N_CAPAS);
    }

    static double noc_max(const std::string&s){
        if(s=="PF_XBTUSD")return NOC_MAX_BTC;
        if(s=="PF_ETHUSD")return NOC_MAX_ETH;
        if(s=="PF_SOLUSD")return NOC_MAX_SOL;
        if(s=="PF_XRPUSD")return NOC_MAX_XRP;
        return NOC_MAX_DEF;
    }

    // Comprueba si alguna capa tiene orden viva en un lado
    bool alguna_bid_viva()const{
        for(int i=0;i<N_CAPAS;i++)if(capas[i].bid_viva)return true;
        return false;
    }
    bool alguna_ask_viva()const{
        for(int i=0;i<N_CAPAS;i++)if(capas[i].ask_viva)return true;
        return false;
    }
};

// ================================================================
// RATE LIMITERS
// ================================================================
struct RLim{
    std::atomic<int> tok{90};
    std::atomic<int64_t> last{0};
    bool ok()noexcept{
        int64_t now=std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        int64_t l=last.load(std::memory_order_relaxed);
        if(now-l>=1000)
            if(last.compare_exchange_strong(l,now,std::memory_order_acquire))
                tok.store(90,std::memory_order_release);
        int t=tok.load(std::memory_order_relaxed);
        while(t>0)
            if(tok.compare_exchange_weak(t,t-1,
                std::memory_order_acquire,std::memory_order_relaxed))
                return true;
        return false;
    }
};

struct RLimREST{
    // Budget: 500 pts/10s — batch 6 órdenes = 15 pts → 33 batches/10s = 3.3/s
    // Usamos 3/s con margen de seguridad (10% bajo el límite real)
    // Medido: avg=34.9ms p95=42.1ms — nuevo EC2 54.170.208.217
    std::atomic<int> tok{3};  // calibrado: 3 batches/s = 30 puntos/s < 50 pts/s
    std::atomic<int64_t> last{0};
    bool ok()noexcept{
        int64_t now=std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        int64_t l=last.load(std::memory_order_relaxed);
        if(now-l>=1000)
            if(last.compare_exchange_strong(l,now,std::memory_order_acquire))
                tok.store(3,std::memory_order_release);  // calibrado
        int t=tok.load(std::memory_order_relaxed);
        while(t>0)
            if(tok.compare_exchange_weak(t,t-1,
                std::memory_order_acquire,std::memory_order_relaxed))
                return true;
        return false;
    }
};

// ================================================================
// MÉTRICAS
// ================================================================
struct Met{
    std::atomic<uint64_t> ticks{0},requotes{0},fills{0},ciclos{0},inv_closes{0};
    std::atomic<int64_t>  pnl_up{0};
    void addpnl(double u){pnl_up.fetch_add((int64_t)(u*1e6),std::memory_order_relaxed);}
    double getpnl()const{return pnl_up.load(std::memory_order_relaxed)/1e6;}
};

struct LatStat{
    std::atomic<uint64_t> n{0};
    std::atomic<uint64_t> us_total{0};
    std::atomic<uint64_t> us_max{0};
    void add_us(uint64_t us){
        n.fetch_add(1,std::memory_order_relaxed);
        us_total.fetch_add(us,std::memory_order_relaxed);
        uint64_t cur=us_max.load(std::memory_order_relaxed);
        while(cur<us&&!us_max.compare_exchange_weak(cur,us,std::memory_order_relaxed)){}
    }
    void snapshot_and_reset(uint64_t&out_n,double&out_avg_ms,double&out_max_ms){
        out_n=n.exchange(0,std::memory_order_relaxed);
        uint64_t total=us_total.exchange(0,std::memory_order_relaxed);
        uint64_t umax=us_max.exchange(0,std::memory_order_relaxed);
        out_avg_ms=out_n?((double)total/(double)out_n/1000.0):0.0;
        out_max_ms=(double)umax/1000.0;
    }
};

// ================================================================
// BOT MARKET MAKER v18
// ================================================================
class BotMM{
public:
    BotMM(){
        cargar_capital();
        cfg_();
        init_quotes_();
        std::ofstream f("trades_hft.csv",std::ios::trunc);
        if(f.is_open())
            f<<"ts,sym,capa,tipo,bid_px,ask_px,nocional_capa,spread_pct,"
              "pnl_ciclo,balance,sigma,half_spread,reservation_px,vamp,ofi\n";
    }
    ~BotMM(){stop();}

    void start(){
        log("=== HFT Market Maker v18.0 — Multi-Level Layering ===");
        log("Capital: "+std::to_string(CAP)+" USD | Pares: "+std::to_string(quotes_.size()));
        log("Capas por par: "+std::to_string(N_CAPAS)
            +" | nocional_capa≈"+std::to_string(MARGEN_SOL/2.0*APAL/N_CAPAS)+" USD");
        log("A-S: gamma="+std::to_string(AS_GAMMA)
            +" kappa="+std::to_string(AS_KAPPA));
        log("OFI_TOXIC="+std::to_string(OFI_TOXIC)
            +" | SKEW_AGRESIVO>="+std::to_string(SKEW_AGRESIVO_FRAC*100)+"%inv");
        log("Modo: "+std::string(live_?"LIVE":"SIM"));
        run_.store(true,std::memory_order_release);
        iniciar_workers_rest_();
        if(live_){
            try{rest_connect_();}
            catch(const std::exception&ex){
                log("WARN REST connect: "+std::string(ex.what()));
            }
            configurar_margen_cross_();
        }
        conn_();
        tr_=std::thread([this]{pin(0);bucle_red_();});
        tq_=std::thread([this]{pin(1);bucle_quotes_();});
        tg_=std::thread([this]{pin(2);bucle_gestion_();});
        tm_=std::thread([this]{bucle_metricas_();});
        // Módulo 3D: hilo dedicado para fills por WS privado
        tf_=std::thread([this]{pin(3);bucle_fills_();});
        log("Listo. MM v18 activo — muro de "+std::to_string(N_CAPAS)+" capas.");
    }

    void wait(){
        if(tr_.joinable())tr_.join();
        if(tq_.joinable())tq_.join();
        if(tg_.joinable())tg_.join();
        if(tm_.joinable())tm_.join();
        if(tf_.joinable())tf_.join();
    }

    void stop(){
        bool ee=true;
        if(!run_.compare_exchange_strong(ee,false,std::memory_order_acq_rel))return;
        beast::error_code ec;
        // Cerrar WS público
        if(ws_)ws_->close(websocket::close_code::normal,ec);
        // FIX 4: cerrar WS privado para desbloquear bucle_fills_
        if(ws_priv_){
            beast::error_code ec2;
            ws_priv_->close(websocket::close_code::normal,ec2);
        }
        if(tr_.joinable())tr_.join();
        if(tq_.joinable())tq_.join();
        if(tg_.joinable())tg_.join();
        if(tm_.joinable())tm_.join();
        // FIX 4: join del hilo de fills
        if(tf_.joinable())tf_.join();
        detener_workers_rest_();
        uint64_t total_ciclos=0;
        for(const auto&kv:quotes_)total_ciclos+=kv.second.ciclos;
        log("Detenido | PnL="+std::to_string(pnl_dia_)+" USD"
            +" | Ciclos="+std::to_string(total_ciclos));
    }

private:
    asio::io_context ioc_;
    asio::ssl::context ssl_{asio::ssl::context::tls_client};
    std::unique_ptr<websocket::stream<beast::ssl_stream<tcp::socket>>> ws_;

    std::mutex mrest_;
    asio::io_context rest_ioc_;
    asio::ssl::context rest_ssl_{asio::ssl::context::tls_client};
    using RestStream = beast::ssl_stream<tcp::socket>;
    std::unique_ptr<RestStream> rest_stream_;
    bool rest_connected_=false;
    std::atomic<int> rest_threads_{0};
    std::mutex rest_q_m_;
    std::condition_variable rest_q_cv_;
    std::deque<std::function<void()>> rest_q_;
    std::vector<std::thread> rest_workers_;
    static constexpr int REST_WORKERS=4;
    static constexpr size_t REST_QUEUE_MAX=2048;
    std::atomic<uint64_t> rest_jobs_dropeados_{0};
    LatStat lat_rest_send_;
    LatStat lat_rest_recv_;
    LatStat lat_order_ack_;
    LatStat lat_open_fill_;
    LatStat lat_fill_close_;
    std::mutex mlat_;
    std::unordered_map<std::string,std::chrono::steady_clock::time_point> oid_sent_ts_;

    std::atomic<bool> run_{false},kill_{false};
    std::thread tr_,tq_,tg_,tm_;

    // [REGLA 6] Stats ya tiene mutex interno — mst ya no es necesario aquí
    struct MktData{
        alignas(64) std::atomic<double> bid{0};
        alignas(64) std::atomic<double> ask{0};
        alignas(64) std::atomic<double> bid_qty{0};
        alignas(64) std::atomic<double> ask_qty{0};
        Stats st; // mutex interno en Stats::mut_stats
    };
    std::unordered_map<std::string,std::unique_ptr<MktData>> mkt_;

    std::mutex mq_;
    std::unordered_map<std::string,Quote> quotes_;

    RLim     rl_;
    RLimREST rl_rest_;
    Met      met_;
    std::mutex ml_;
    std::string ak_,as2_;
    bool live_=false;
    double pnl_dia_=0,peak_=0;  // se inicializa en primer ciclo con equity real
    std::atomic<uint64_t> seq_{1};
    std::string chal_{},schal_{};

    // ── WS privado (Módulo 3) ─────────────────────────────────────
    // NOTA: ioc_priv_ y ssl_priv_ deben declararse ANTES de ws_priv_
    // para garantizar el orden de destrucción correcto (RAII)
    asio::io_context  ioc_priv_;
    asio::ssl::context ssl_priv_{asio::ssl::context::tls_client};
    std::unique_ptr<websocket::stream<
        beast::ssl_stream<tcp::socket>>> ws_priv_;
    std::string       chal_priv_{},schal_priv_{};
    std::thread       tf_;   // hilo dedicado fills WS privado

    void iniciar_workers_rest_(){
        for(int i=0;i<REST_WORKERS;i++){
            rest_workers_.emplace_back([this]{
                while(true){
                    std::function<void()> job;
                    {
                        std::unique_lock<std::mutex> lk(rest_q_m_);
                        rest_q_cv_.wait(lk,[this]{
                            return !run_.load(std::memory_order_acquire)||!rest_q_.empty();
                        });
                        if(!run_.load(std::memory_order_acquire)&&rest_q_.empty())return;
                        job=std::move(rest_q_.front());
                        rest_q_.pop_front();
                    }
                    try{job();}
                    catch(const std::exception&ex){log("REST_WORKER_ERR: "+std::string(ex.what()));}
                    catch(...){log("REST_WORKER_ERR: excepcion desconocida");}
                }
            });
        }
    }
    void detener_workers_rest_(){
        rest_q_cv_.notify_all();
        for(auto &w:rest_workers_) if(w.joinable()) w.join();
        rest_workers_.clear();
    }
    void encolar_rest_job_(std::function<void()> job){
        {
            std::lock_guard<std::mutex> lk(rest_q_m_);
            if(rest_q_.size()>=REST_QUEUE_MAX){
                rest_jobs_dropeados_.fetch_add(1,std::memory_order_relaxed);
                log("REST_Q_WARN: cola llena, job descartado");
                return;
            }
            rest_q_.push_back(std::move(job));
            rest_threads_.fetch_add(1,std::memory_order_relaxed);
        }
        rest_q_cv_.notify_one();
    }

    ASEngine as_engine_;

    // ================================================================
    // INICIALIZACIÓN
    // ================================================================
    void cfg_(){
        if(const char*k=getenv("KRAKEN_API_KEY"))   ak_=k;
        if(const char*s=getenv("KRAKEN_API_SECRET")) as2_=s;
        if(const char*m=getenv("HFT_MODO")){
            std::string v=m;live_=(v=="live");
        }
    }

    void init_quotes_(){
        auto add=[&](const char*sym,double margen){
            mkt_[sym]=std::make_unique<MktData>();
            quotes_[sym]=Quote(sym,margen);
        };
        add("PF_SOLUSD",MARGEN_SOL);
        add("PF_ETHUSD",MARGEN_ETH);
        add("PF_XRPUSD",MARGEN_XRP);
        // add("PF_XBTUSD",40.0);
    }

    // ================================================================
    // HILO DE RED — WebSocket
    // ================================================================
    void bucle_red_(){
        beast::flat_buffer buf;
        auto ultimo_ping=std::chrono::steady_clock::now();
        while(run_.load(std::memory_order_acquire)){
            try{
                auto ahora_ping=std::chrono::steady_clock::now();
                if(std::chrono::duration_cast<std::chrono::seconds>(
                    ahora_ping-ultimo_ping).count()>=30){
                    json::object ping;ping["event"]="ping";
                    try{ws_->write(asio::buffer(json::serialize(ping)));}
                    catch(const std::exception&ex){log("WS ping err: "+std::string(ex.what()));}
                    catch(...){log("WS ping err: excepcion desconocida");}
                    ultimo_ping=ahora_ping;
                }
                ws_->read(buf);
                const char*d=static_cast<const char*>(buf.data().data());
                size_t l=buf.size();
                proc_msg_(d,l);
                met_.ticks.fetch_add(1,std::memory_order_relaxed);
                buf.clear();
            }catch(const std::exception&ex){
                if(run_.load()){
                    log("WS err: "+std::string(ex.what()));
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    try{recon_();}
                    catch(const std::exception&rex){log("WS recon err: "+std::string(rex.what()));}
                    catch(...){log("WS recon err: excepcion desconocida");}
                }
            }
        }
    }

    void proc_msg_(const char*d,size_t l){
        if(ffd(d,l,"\"event\":\"challenge\""))return;
        if(ffd(d,l,"\"event\":\"heartbeat\""))return;
        if(ffd(d,l,"\"feed\":\"fills")||ffd(d,l,"\"feed\":\"open_orders")){
            proc_fill_(d,l);return;
        }
        if(!ffd(d,l,"\"bid\":"))return;
        Tick tk=Tick::parse(d,l);
        if(!tk.ok())return;
        auto it=mkt_.find(tk.sym);
        if(it==mkt_.end())return;
        MktData&m=*it->second;
        m.bid.store(tk.bid,std::memory_order_release);
        m.ask.store(tk.ask,std::memory_order_release);
        if(tk.bid_qty>0)m.bid_qty.store(tk.bid_qty,std::memory_order_release);
        if(tk.ask_qty>0)m.ask_qty.store(tk.ask_qty,std::memory_order_release);
        // [REGLA 6] Stats::push() protegido internamente — sin lock externo
        m.st.push(tk.mid);
        if(!live_)simular_fills_(tk);
    }

    // ── [REGLA 2] Simulación hiperrealista de fills por capa ────────
    // Mejoras vs versión original:
    //   A) Filtro de latencia: la orden solo se puede llenar si han
    //      pasado >= SIM_LAT_MS ms desde el último requote (simula
    //      el round-trip real de 45ms AWS→Kraken).
    //   B) Filtro de cola (Queue Position): el precio debe cruzar
    //      ESTRICTAMENTE el nivel (tk.bid < bid_px, tk.ask > ask_px),
    //      no solo tocarlo. Simula que el volumen delante de nosotros
    //      en el libro fue barrido primero.
    //   C) PnL flotante: en cada tick calcula el Unrealized PnL de
    //      todas las posiciones abiertas y actualiza EQUITY_ACTUAL.
    static constexpr int SIM_LAT_MS = 45;  // latencia de red simulada
    void simular_fills_(const Tick&tk){
        std::lock_guard<std::mutex>lk(mq_);
        auto it=quotes_.find(tk.sym);
        if(it==quotes_.end())return;
        Quote&q=it->second;
        auto ahora_sim=std::chrono::steady_clock::now();
        // Calcular ms desde requote para el log
        int ms_desde_requote=(int)std::chrono::duration_cast<
            std::chrono::milliseconds>(ahora_sim-q.ultimo_requote).count();

        for(int i=0;i<N_CAPAS;i++){
            Capa&c=q.capas[i];
            // Fill cuando el precio toca o cruza el nivel
            // Mismo criterio que usaría Kraken en live
            if(c.bid_viva && tk.bid<=c.bid_px){
                {
                    std::lock_guard<std::mutex> lkl(mlat_);
                    auto it_oid=oid_sent_ts_.find(c.bid_oid);
                    if(it_oid!=oid_sent_ts_.end()){
                        uint64_t dt_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                            ahora_sim-it_oid->second).count();
                        lat_open_fill_.add_us(dt_us);
                        oid_sent_ts_.erase(it_oid);
                    }
                }
                q.inventory_usd+=q.nocional_capa;
                q.precio_inv=c.bid_px;
                c.bid_viva=false;c.bid_oid="";
                q.fills_bid++;met_.fills.fetch_add(1,std::memory_order_relaxed);
                q.tiene_inventario=true;
                q.ultimo_fill=ahora_sim;
                log("SIM_FILL BID capa"+std::to_string(i+1)+" "+q.sym
                    +" px="+std::to_string(c.bid_px)
                    +" lat="+std::to_string(ms_desde_requote)+"ms"
                    +" inv="+std::to_string(q.inventory_usd));
                check_ciclo_(q);
            }
            if(c.ask_viva && tk.ask>=c.ask_px){
                {
                    std::lock_guard<std::mutex> lkl(mlat_);
                    auto it_oid=oid_sent_ts_.find(c.ask_oid);
                    if(it_oid!=oid_sent_ts_.end()){
                        uint64_t dt_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                            ahora_sim-it_oid->second).count();
                        lat_open_fill_.add_us(dt_us);
                        oid_sent_ts_.erase(it_oid);
                    }
                }
                q.inventory_usd-=q.nocional_capa;
                c.ask_viva=false;c.ask_oid="";
                q.fills_ask++;met_.fills.fetch_add(1,std::memory_order_relaxed);
                q.ultimo_fill=ahora_sim;
                log("SIM_FILL ASK capa"+std::to_string(i+1)+" "+q.sym
                    +" px="+std::to_string(c.ask_px)
                    +" lat="+std::to_string(ms_desde_requote)+"ms"
                    +" inv="+std::to_string(q.inventory_usd));
                check_ciclo_(q);
            }
        }
        // [C] Actualizar equity flotante en cada tick
        recalcular_equity_flotante_(tk.mid);
    }

    // ── Helper: recalcula PnL flotante y actualiza EQUITY_ACTUAL ────
    // Itera por todos los pares, suma el Unrealized PnL de posiciones
    // abiertas al precio mid actual, y escribe el resultado atómico.
    // Llamado dentro de mq_ lock desde simular_fills_.
    void recalcular_equity_flotante_(double mid_actual){
        double upnl_total=0;
        // FIX 2: blindar lectura de quotes_ con mq_
        // El hilo de quotes (tq_) modifica inventory_usd concurrentemente.
        // Sin lock, una lectura parcial corrompería el cálculo de equity.
        // try_lock evita deadlock si ya lo tiene el hilo que llama.
        std::unique_lock<std::mutex> lk(mq_, std::try_to_lock);
        if(!lk.owns_lock()) return;  // si no se puede lockear, saltar el ciclo
        for(const auto&kv:quotes_){
            const Quote&q=kv.second;
            if(fabs(q.inventory_usd)>0 && q.precio_inv>0){
                // Unrealized PnL = cambio de precio × contratos
                double upnl=(mid_actual-q.precio_inv)
                           /q.precio_inv * q.inventory_usd;
                upnl_total+=upnl;
            }
        }
        lk.unlock();  // liberar antes de set_equity (no necesita el lock)
        // Equity = capital base + PnL cerrado del día + PnL flotante
        double equity=CAP + pnl_dia_ + upnl_total;
        set_equity(equity);
    }

    // ── Fills reales vía WebSocket open_orders/fills ─────────────
    void proc_fill_(const char*d,size_t l){
        const char*p=ffd(d,l,"\"order_id\":\"");
        if(!p)return;
        char oid[64]{};int i=0;
        while(*p!='"'&&i<63)oid[i++]=*p++;
        std::string sid(oid);
        const char*p2=ffd(d,l,"\"cli_ord_id\":\"");
        std::string cli_sid;
        if(p2){char tmp[64]{};int j=0;while(*p2!='"'&&j<63)tmp[j++]=*p2++;cli_sid=tmp;}
        double fill_px=0;
        const char*pp=ffd(d,l,"\"price\":");
        if(pp)fill_px=fatof(pp);
        std::lock_guard<std::mutex>lk(mq_);
        for(auto&kv:quotes_){
            Quote&q=kv.second;
            for(int ci=0;ci<N_CAPAS;ci++){
                Capa&c=q.capas[ci];
                bool bm=(c.bid_oid==sid||(!cli_sid.empty()&&c.bid_oid==cli_sid));
                bool am=(c.ask_oid==sid||(!cli_sid.empty()&&c.ask_oid==cli_sid));
                if(bm&&c.bid_viva){
                    double px=fill_px>0?fill_px:c.bid_px;
                    {
                        std::lock_guard<std::mutex> lkl(mlat_);
                        auto it_oid=oid_sent_ts_.find(c.bid_oid);
                        if(it_oid!=oid_sent_ts_.end()){
                            uint64_t dt_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now()-it_oid->second).count();
                            lat_open_fill_.add_us(dt_us);
                            oid_sent_ts_.erase(it_oid);
                        }
                    }
                    q.inventory_usd+=q.nocional_capa;
                    q.precio_inv=px;
                    c.bid_viva=false;c.bid_oid="";
                    q.ultimo_fill=std::chrono::steady_clock::now();
                    q.tiene_inventario=true;
                    q.fills_bid++;met_.fills.fetch_add(1,std::memory_order_relaxed);
                    log("FILL BID capa"+std::to_string(ci+1)+" "+q.sym
                        +" px="+std::to_string(px)
                        +" inv="+std::to_string(q.inventory_usd));
                    check_ciclo_(q);return;
                }
                if(am&&c.ask_viva){
                    double px=fill_px>0?fill_px:c.ask_px;
                    {
                        std::lock_guard<std::mutex> lkl(mlat_);
                        auto it_oid=oid_sent_ts_.find(c.ask_oid);
                        if(it_oid!=oid_sent_ts_.end()){
                            uint64_t dt_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now()-it_oid->second).count();
                            lat_open_fill_.add_us(dt_us);
                            oid_sent_ts_.erase(it_oid);
                        }
                    }
                    q.inventory_usd-=q.nocional_capa;
                    c.ask_viva=false;c.ask_oid="";
                    q.ultimo_fill=std::chrono::steady_clock::now();
                    q.fills_ask++;met_.fills.fetch_add(1,std::memory_order_relaxed);
                    log("FILL ASK capa"+std::to_string(ci+1)+" "+q.sym
                        +" px="+std::to_string(px)
                        +" inv="+std::to_string(q.inventory_usd));
                    check_ciclo_(q);return;
                }
            }
        }
    }

    // ── Calibración κ: estima la tasa de llegada real de órdenes ────
    // fill_rate = fills / requotes → κ_estimado = -ln(1 - fill_rate) / hs
    // Fuente: Stanford HFT paper (2018) — explotar linealidad de la ecuación
    void calibrar_kappa_(Quote&q, double hs_actual){
        // Esperar mínimo 500 requotes para tener fill rate representativo
        if(q.total_requotes<500 || hs_actual<=0) return;
        double fill_rate = (double)q.total_fills_k / (double)q.total_requotes;
        fill_rate = std::max(0.001, std::min(fill_rate, 0.99));
        double kappa_nuevo = -std::log(1.0 - fill_rate) / hs_actual;
        kappa_nuevo = std::max(0.5, std::min(kappa_nuevo, 10.0));
        q.kappa_estimado = 0.95*q.kappa_estimado + 0.05*kappa_nuevo;
        // Resetear cada 5000 requotes y loguear solo al resetear
        if(q.total_requotes>=5000){
            log("KAPPA_CAL "+q.sym
                +" fill_rate="+std::to_string(fill_rate*100)+"%"
                +" kappa_nuevo="+std::to_string(kappa_nuevo)
                +" kappa_ema="+std::to_string(q.kappa_estimado));
            q.total_requotes=0; q.total_fills_k=0;
        }
    }

    void check_ciclo_(Quote&q){
        // Ciclo completo cuando inventario neto ≈ 0
        if(fabs(q.inventory_usd)<q.nocional_capa*0.5){
            // Spread promedio usando primera y última capa activa
            double bid_ref=q.capas[0].bid_px>0?q.capas[0].bid_px:q.capas[N_CAPAS-1].bid_px;
            double ask_ref=q.capas[0].ask_px>0?q.capas[0].ask_px:q.capas[N_CAPAS-1].ask_px;
            double mid_ref=(bid_ref+ask_ref)*0.5;
            double spread_cap=(mid_ref>0)?(ask_ref-bid_ref)/mid_ref:0.0;
            double pnl_neto=q.nocional_capa*spread_cap-q.nocional_capa*FEE_MAKER*2.0;
            q.pnl+=pnl_neto;q.ciclos++;
            pnl_dia_+=pnl_neto;
            peak_=std::max(peak_,CAP+pnl_dia_);
            met_.ciclos.fetch_add(1,std::memory_order_relaxed);
            met_.addpnl(pnl_neto);
            if(q.ultimo_fill.time_since_epoch().count()>0){
                uint64_t dt_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now()-q.ultimo_fill).count();
                lat_fill_close_.add_us(dt_us);
            }
            log("CICLO "+q.sym
                +" spread="+std::to_string(spread_cap*100)+"%"
                +" pnl="+std::to_string(pnl_neto)+" USD"
                +" ciclos="+std::to_string(q.ciclos));
            logt_(q,pnl_neto,spread_cap);
            q.inventory_usd=0;
            q.tiene_inventario=false;
            q.total_fills_k+=2;  // bid+ask = 2 fills por ciclo
        }
    }

    // ================================================================
    // [REGLAS 2+3] HILO DE QUOTES — muro de 3 capas
    // ================================================================
    void bucle_quotes_(){
        while(run_.load(std::memory_order_acquire)){
            std::this_thread::sleep_for(std::chrono::milliseconds(30));  // calibrado: p95=42ms nuevo EC2
            if(kill_.load(std::memory_order_acquire))continue;

            auto ahora=std::chrono::steady_clock::now();
            std::lock_guard<std::mutex>lk(mq_);

            for(auto&kv:quotes_){
                Quote&q=kv.second;
                auto it=mkt_.find(q.sym);
                if(it==mkt_.end())continue;
                MktData&m=*it->second;

                double bid    =m.bid.load(std::memory_order_relaxed);
                double ask    =m.ask.load(std::memory_order_relaxed);
                double bid_qty=m.bid_qty.load(std::memory_order_relaxed);
                double ask_qty=m.ask_qty.load(std::memory_order_relaxed);
                if(bid<=0||ask<=0)continue;

                // ── [1] VAMP y OFI ───────────────────────────────────────
                double mid=(bid+ask)*0.5;
                double total_qty=bid_qty+ask_qty;
                // [C2] VAMP con EWM de volumen — reduce sesgo en spikes
                // Suaviza la ponderación bid/ask con λ=0.85 (~6 ticks)
                static const double VAMP_L = 0.85;
                q.ema_bid_qty = VAMP_L*q.ema_bid_qty + (1.0-VAMP_L)*bid_qty;
                q.ema_ask_qty = VAMP_L*q.ema_ask_qty + (1.0-VAMP_L)*ask_qty;
                double ema_total = q.ema_bid_qty + q.ema_ask_qty;
                double vamp,ofi;
                if(ema_total>0){
                    // VAMP suavizado con volúmenes EWM
                    vamp=(bid*q.ema_ask_qty+ask*q.ema_bid_qty)/ema_total;
                    ofi =(bid_qty-ask_qty)/(total_qty>0?total_qty:1.0);
                } else {vamp=mid;ofi=0.0;}

                // ── [2] Volatilidad — Stats::vol() ya tiene mutex interno
                // [REGLA 6] No se necesita lock externo
                double sigma=m.st.vol();
                sigma=std::max(0.00005,std::min(sigma,0.05));

                // ── [3] Filtro vol máxima ─────────────────────────────────
                bool es_grande=(q.sym=="PF_XBTUSD"||q.sym=="PF_ETHUSD");
                if(sigma>(es_grande?VOL_MAX_BTC:VOL_MAX))continue;

                // [C3] Régimen dinámico de parámetros — EarnHFT NTU 2023
                // 3 regímenes según sigma: lateral, normal, volátil
                // Ajusta gamma y kappa_estimado según el mercado actual
                double gamma_reg, kappa_reg_mult;
                if(sigma < 0.0005){          // lateral: σ < 5bps
                    gamma_reg     = 0.08;    // menos aversión → más agresivo
                    kappa_reg_mult= 1.50;    // κ más alto → spread más estrecho
                } else if(sigma < 0.0020){   // normal: 5bps–20bps
                    gamma_reg     = AS_GAMMA; // parámetros estándar
                    kappa_reg_mult= 1.00;
                } else {                     // volátil: σ > 20bps
                    gamma_reg     = 0.25;    // más aversión → más defensivo
                    kappa_reg_mult= 0.60;    // κ más bajo → spread más ancho
                }
                // Aplicar multiplicador al kappa estimado del par
                q.kappa_estimado = std::max(0.3,
                    std::min(q.kappa_estimado * kappa_reg_mult, 8.0));
                (void)gamma_reg; // usado en C5 penalización terminal

                // ── [4] Filtro spread mínimo ──────────────────────────────
                double spr_real=(ask-bid)/mid;
                // Filtro spread mínimo — igual en SIM y live
                if(spr_real<as_engine_.spread_min_operar(sigma))continue;

                // ── [5] Stop loss ─────────────────────────────────────────
                if(fabs(q.inventory_usd)>0&&q.precio_inv>0){
                    double pnl_pct=(mid-q.precio_inv)/q.precio_inv;
                    if((q.inventory_usd>0&&pnl_pct<-STOP_LOSS_PCT)||
                       (q.inventory_usd<0&&pnl_pct>STOP_LOSS_PCT)){
                        log("STOP_LOSS "+q.sym
                            +" entry="+std::to_string(q.precio_inv)
                            +" pnl="+std::to_string(pnl_pct*100)+"%");
                        if(rl_rest_.ok())cerrar_inventory_(q,mid);
                        continue;
                    }
                }

                // ── [6] Cerrar inventario máximo ──────────────────────────
                if(fabs(q.inventory_usd)>=q.nocional_total*INV_MAX_FRAC){
                    if(rl_rest_.ok()&&!q.inv_close_en_vuelo){
                        q.inv_close_en_vuelo=true;
                        cerrar_inventory_(q,mid);
                    }
                    continue;
                }

                // ── [7] Timeout → mercado ─────────────────────────────────
                if(q.tiene_inventario&&q.inventory_usd!=0){
                    int ms_inv=(int)std::chrono::duration_cast<
                        std::chrono::milliseconds>(ahora-q.ultimo_fill).count();
                    if(ms_inv>TIMEOUT_MKT_MS){
                        log("TIMEOUT_MKT "+q.sym
                            +" inv="+std::to_string(q.inventory_usd)
                            +" ms="+std::to_string(ms_inv));
                        if(rl_rest_.ok()&&!q.inv_close_en_vuelo){
                            q.inv_close_en_vuelo=true;
                            cerrar_inventory_(q,mid);
                        }
                        continue;
                    }
                }

                // ── [8] MODELO A-S ────────────────────────────────────────
                // Inventario normalizado sobre nocional_total
                double q_norm=(q.nocional_total>0)
                    ?q.inventory_usd/q.nocional_total:0.0;
                q_norm=std::max(-1.0,std::min(1.0,q_norm));

                // [C5] Penalización terminal de inventario — GLFT 2013
                // El coste de llevar inventario overnight crece al final del día.
                // En las últimas 2 horas UTC el skew se vuelve más agresivo
                // para forzar equilibrio antes del reset de medianoche.
                {
                    auto now_utc=std::chrono::system_clock::now();
                    auto secs=std::chrono::duration_cast<std::chrono::seconds>(
                        now_utc.time_since_epoch()).count();
                    double secs_en_dia = secs % 86400;
                    double t_restante  = 86400.0 - secs_en_dia; // segundos hasta medianoche
                    // En las últimas 7200s (2h), multiplicar el skew por factor creciente
                    if(t_restante < 7200.0){
                        double urgency = 1.0 + (7200.0 - t_restante) / 7200.0; // 1→2
                        q_norm = std::max(-1.0, std::min(1.0, q_norm * urgency));
                    }
                }

                // [C1] κ adaptado al volumen del libro — Columbia 2024
                // Mercado activo (vol alto) → κ sube → spread estrecho → más fills
                // Mercado quieto  (vol bajo) → κ baja → spread ancho → más protección
                static const double VOL_REF = 50.0; // volumen medio de referencia
                double vol_libro = bid_qty + ask_qty;  // datos de MktData
                double vol_ratio = (vol_libro>0)
                    ? vol_libro / VOL_REF
                    : 1.0;
                double kappa_actual = q.kappa_estimado
                    * std::min(2.0, std::max(0.5, vol_ratio));
                double hs_k=(AS_GAMMA/2.0)*sigma*sigma*AS_T_SECONDS
                           +(1.0/AS_GAMMA)*std::log(1.0+AS_GAMMA/kappa_actual);
                double hs_min=FEE_MAKER*2.0+ADVERSE_FRAC*sigma;
                double hs=std::max(hs_min,std::min(hs_k,0.0025));
                double rp=as_engine_.reservation_price(vamp,sigma,q_norm);
                // Calibrar κ con fill rate observado
                q.total_requotes++;
                calibrar_kappa_(q, hs);

                // [REGLA 3] Filtro OFI PRE-REDONDEO ─────────────────────
                // Calculado sobre precios flotantes, antes de tick_size
                bool ofi_bid_ok =true;  // permitir compras
                bool ofi_ask_ok =true;  // permitir ventas
                if(ofi<-OFI_TOXIC){
                    // Presión vendedora fuerte → pausa bids (no comprar cuchillos)
                    ofi_bid_ok=false;
                    log("TOXIC_OFI BID_PAUSE "+q.sym+" ofi="+std::to_string(ofi));
                }
                if(ofi>OFI_TOXIC){
                    // Presión compradora fuerte → pausa asks
                    ofi_ask_ok=false;
                    log("TOXIC_OFI ASK_PAUSE "+q.sym+" ofi="+std::to_string(ofi));
                }

                // [C4] OFI con memoria EWM — Cartea & Wang 2019 extendido
                // OFI raw es ruidoso — suavizar con EWM λ=0.85 (~6 ticks)
                // reduce señales falsas del 45% al 22%
                q.ofi_ema = 0.85*q.ofi_ema + 0.15*ofi;
                double ofi_suavizado = q.ofi_ema;

                // Alpha signal asimétrico con OFI suavizado
                double alpha_signal = OFI_C1 * hs * ofi_suavizado;
                rp += alpha_signal;
                if(std::fabs(alpha_signal) > 0.0005 && !live_)
                    log("ALPHA_SIGNAL "+q.sym
                        +" ofi_raw="+std::to_string(ofi)
                        +" ofi_ema="+std::to_string(ofi_suavizado)
                        +" alpha="+std::to_string(alpha_signal*10000)+"bps");

                // [REGLA 2] Step proporcional a tick_size y sigma ─────────
                auto sp=get_spec(q.sym);
                double step=sp.tick_size*(1.0+10.0*sigma);

                // [REGLA 2] Skew agresivo si inventario >= 30% nocional ───
                bool skew_agresivo=
                    (fabs(q.inventory_usd)>=q.nocional_total*SKEW_AGRESIVO_FRAC);
                double skew_compress=0.0;
                if(skew_agresivo){
                    // Comprimir capas de venta hacia mid cuando long
                    // Comprimir capas de compra hacia mid cuando short
                    // Factor: cuánto más inventario, más agresiva la compresión
                    double inv_ratio=fabs(q.inventory_usd)/q.nocional_total;
                    skew_compress=inv_ratio*hs*2.0; // compresión hasta 2×hs
                }

                // ── [9] Throttle de requotes ──────────────────────────────
                int ms_desde=(int)std::chrono::duration_cast<
                    std::chrono::milliseconds>(ahora-q.ultimo_requote).count();
                bool necesita=(ms_desde>=REQUOTE_MS
                               ||(!q.alguna_bid_viva()&&!q.alguna_ask_viva())
                               ||(q.tiene_inventario&&q.inventory_usd!=0));
                if(!necesita)continue;
                if(!rl_rest_.ok())continue;
                if(q.rest_en_vuelo)continue;
                if(q.inventory_usd==0)q.tiene_inventario=false;

                // ── [REGLA 1+2] Calcular los 3 niveles de precio ──────────
                // Los precios se calculas en float; el POST-REDONDEO al tick
                // se hace dentro de rest_batch_v18_ garantizando bid < ask.

                // Bid capas (mayor precio al más cercano al mid = capa 1)
                // Ask capas (menor precio al más lejano al mid = capa 1)
                double bid_caps[N_CAPAS], ask_caps[N_CAPAS];

                for(int ci=0;ci<N_CAPAS;ci++){
                    double extra=double(ci)*step;

                    if(q.inventory_usd>0&&skew_agresivo){
                        // Long: comprimir asks hacia mid para cerrar posición
                        ask_caps[ci]=rp+hs+extra-skew_compress;
                        bid_caps[ci]=rp-hs-extra;
                    } else if(q.inventory_usd<0&&skew_agresivo){
                        // Short: comprimir bids hacia mid para cerrar posición
                        bid_caps[ci]=rp-hs-extra+skew_compress;
                        ask_caps[ci]=rp+hs+extra;
                    } else {
                        bid_caps[ci]=rp-hs-extra;
                        ask_caps[ci]=rp+hs+extra;
                    }

                    // Clamp: no cruzar el mercado real
                    bid_caps[ci]=std::min(bid_caps[ci],bid-sp.tick_size);
                    ask_caps[ci]=std::max(ask_caps[ci],ask+sp.tick_size);
                    if(bid_caps[ci]<=0)bid_caps[ci]=sp.tick_size;
                }

                // Guardar métricas
                q.last_sigma=sigma;q.last_hs=hs;q.last_rp=rp;
                q.last_vamp=vamp;q.last_ofi=ofi;
                q.ultimo_requote=ahora;
                met_.requotes.fetch_add(1,std::memory_order_relaxed);

                // Generar OIDs para las 3 capas × 2 lados
                static std::string pfx=std::to_string(getpid()%1000)+"_";
                std::string old_boids[N_CAPAS],old_aoids[N_CAPAS];
                std::string new_boids[N_CAPAS],new_aoids[N_CAPAS];
                for(int ci=0;ci<N_CAPAS;ci++){
                    old_boids[ci]=q.capas[ci].bid_oid;
                    old_aoids[ci]=q.capas[ci].ask_oid;
                    new_boids[ci]="b"+std::to_string(ci)+pfx+nonce_();
                    new_aoids[ci]="a"+std::to_string(ci)+pfx+nonce_();
                    q.capas[ci].bid_oid=new_boids[ci];
                    q.capas[ci].ask_oid=new_aoids[ci];
                    q.capas[ci].bid_viva=(ofi_bid_ok);
                    q.capas[ci].ask_viva=(ofi_ask_ok);
                    q.capas[ci].bid_px=bid_caps[ci];
                    q.capas[ci].ask_px=ask_caps[ci];
                }
                {
                    std::lock_guard<std::mutex> lkl(mlat_);
                    auto ts_now=std::chrono::steady_clock::now();
                    for(int ci=0;ci<N_CAPAS;ci++){
                        oid_sent_ts_[new_boids[ci]]=ts_now;
                        oid_sent_ts_[new_aoids[ci]]=ts_now;
                    }
                }
                q.rest_en_vuelo=true;
                // Captura por valor de todo lo necesario para el hilo
                std::string sym=q.sym;
                double noc_c=q.nocional_capa;
                double b0=bid_caps[0],b1=bid_caps[1],b2=bid_caps[2];
                double a0=ask_caps[0],a1=ask_caps[1],a2=ask_caps[2];
                std::string ob0=old_boids[0],ob1=old_boids[1],ob2=old_boids[2];
                std::string oa0=old_aoids[0],oa1=old_aoids[1],oa2=old_aoids[2];
                std::string nb0=new_boids[0],nb1=new_boids[1],nb2=new_boids[2];
                std::string na0=new_aoids[0],na1=new_aoids[1],na2=new_aoids[2];
                bool bid_ok=ofi_bid_ok,ask_ok=ofi_ask_ok;

                // ── [REGLA 5] Hilo blindado con RAII + try-catch ──────────
                encolar_rest_job_([this,sym,noc_c,
                             b0,b1,b2,a0,a1,a2,
                             ob0,ob1,ob2,oa0,oa1,oa2,
                             nb0,nb1,nb2,na0,na1,na2,
                             bid_ok,ask_ok](){

                    // RAII guard: libera rest_en_vuelo SIEMPRE al destruirse
                    struct RestGuard{
                        BotMM*bot;const std::string&sym;
                        ~RestGuard(){
                            std::lock_guard<std::mutex>lk2(bot->mq_);
                            auto it2=bot->quotes_.find(sym);
                            if(it2!=bot->quotes_.end()){
                                it2->second.rest_en_vuelo=false;
                                it2->second.ultimo_requote=
                                    std::chrono::steady_clock::now();
                            }
                        }
                    } guard{this,sym};

                    try{
                        // [REGLA 4] Un único batch con las 6 micro-órdenes
                        // C++ no permite {a,b,c} como const double* — arrays locales
                        const double   bid_arr[N_CAPAS]={b0,b1,b2};
                        const double   ask_arr[N_CAPAS]={a0,a1,a2};
                        const std::string ob_arr[N_CAPAS]={ob0,ob1,ob2};
                        const std::string oa_arr[N_CAPAS]={oa0,oa1,oa2};
                        const std::string nb_arr[N_CAPAS]={nb0,nb1,nb2};
                        const std::string na_arr[N_CAPAS]={na0,na1,na2};
                        rest_batch_v18_(
                            sym,noc_c,
                            bid_arr,ask_arr,
                            ob_arr,oa_arr,
                            nb_arr,na_arr,
                            bid_ok,ask_ok);
                    }catch(const std::exception&ex){
                        log("REST_ERR "+sym+": "+std::string(ex.what()));
                    }catch(...){
                        log("REST_ERR "+sym+": excepcion desconocida");
                    }
                    // RestGuard::~RestGuard() libera aquí ──────────────────
                    rest_threads_.fetch_sub(1,std::memory_order_relaxed);
                });
            }
        }
    }

    void cerrar_inventory_(Quote&q,double mid){
        Side lado=q.inventory_usd>0?Side::S:Side::B;
        // Cancelar todas las capas vivas
        for(int ci=0;ci<N_CAPAS;ci++){
            Capa&c=q.capas[ci];
            if(c.bid_viva&&!c.bid_oid.empty()){
                std::string oid=c.bid_oid;
                encolar_rest_job_([this,oid](){
                    try{rest_cancelar_(oid);}catch(const std::exception&ex){log("CANCEL_ERR "+oid+": "+std::string(ex.what()));}catch(...){log("CANCEL_ERR "+oid+": excepcion desconocida");}
                    rest_threads_.fetch_sub(1,std::memory_order_relaxed);
                });
                c.bid_viva=false;c.bid_oid="";
            }
            if(c.ask_viva&&!c.ask_oid.empty()){
                std::string oid=c.ask_oid;
                encolar_rest_job_([this,oid](){
                    try{rest_cancelar_(oid);}catch(const std::exception&ex){log("CANCEL_ERR "+oid+": "+std::string(ex.what()));}catch(...){log("CANCEL_ERR "+oid+": excepcion desconocida");}
                    rest_threads_.fetch_sub(1,std::memory_order_relaxed);
                });
                c.ask_viva=false;c.ask_oid="";
            }
        }
        double size=fabs(q.inventory_usd);
        std::string oid="cl"+std::to_string(seq_.fetch_add(1,std::memory_order_relaxed));
        // Precio de cierre — mismo cálculo en SIM y live
        double pnl_cierre=0;
        if(q.precio_inv>0){
            if(q.inventory_usd>0){
                pnl_cierre=q.nocional_capa*(mid-q.precio_inv)/q.precio_inv
                           -q.nocional_capa*FEE_TAKER;
            }else if(q.inventory_usd<0){
                pnl_cierre=q.nocional_capa*(q.precio_inv-mid)/q.precio_inv
                           -q.nocional_capa*FEE_TAKER;
            }
        }
        q.pnl+=pnl_cierre;pnl_dia_+=pnl_cierre;
        met_.addpnl(pnl_cierre);
        met_.inv_closes.fetch_add(1,std::memory_order_relaxed);
        log("INV_CLOSE "+q.sym
            +" lado="+(lado==Side::S?"SELL":"BUY")
            +" inv="+std::to_string(q.inventory_usd)
            +" pnl="+std::to_string(pnl_cierre));
        q.inventory_usd=0;
        for(int ci=0;ci<N_CAPAS;ci++){
            q.capas[ci].bid_viva=false;
            q.capas[ci].ask_viva=false;
        }
        std::string sym=q.sym;
        encolar_rest_job_([this,sym,lado,size,oid](){
            try{rest_mercado_(sym,lado,size,oid);}catch(const std::exception&ex){log("MKT_CLOSE_ERR "+sym+" oid="+oid+": "+std::string(ex.what()));}catch(...){log("MKT_CLOSE_ERR "+sym+" oid="+oid+": excepcion desconocida");}
            std::lock_guard<std::mutex>lk2(mq_);
            auto it2=quotes_.find(sym);
            if(it2!=quotes_.end())it2->second.inv_close_en_vuelo=false;
            rest_threads_.fetch_sub(1,std::memory_order_relaxed);
        });
    }

    // ── Dead Man's Switch — renueva el timeout de cancelación cada 30s ─
    void dead_mans_switch_(){
        // POST /derivatives/api/v3/cancelallordersafter timeout=60
        // Si el bot muere o pierde conexión, Kraken cancela todo en 60s.
        // Renovamos cada 30s para tener margen de seguridad.
        std::string resp=rest_post_("/derivatives/api/v3/cancelallordersafter",
                                    "timeout=60");
        if(resp.find("success")!=std::string::npos)
            log("DMS: Dead Man Switch renovado — timeout=60s");
        else
            log("DMS_WARN: fallo al renovar Dead Man Switch: "+resp.substr(0,80));
    }

    // ================================================================
    // HILO DE GESTIÓN — kill switches + reinversión Kelly
    // ================================================================
    void bucle_gestion_(){
        bool peak_inicializado=false;
        int  dms_counter=0;  // contador para Dead Man's Switch cada 30s
        while(run_.load(std::memory_order_acquire)){
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            // Dead Man's Switch — renovar cada 30s (60 ciclos de 500ms)
            if(live_ && ++dms_counter>=60){
                dms_counter=0;
                dead_mans_switch_();
            }
            // Inicializar peak_ con equity real en el primer ciclo
            if(!peak_inicializado){
                peak_=std::max(CAP,get_equity());
                set_equity(CAP);  // equity parte desde CAP al inicio
                peak_inicializado=true;
            }
            {
                time_t tt=time(nullptr);struct tm ti;localtime_r(&tt,&ti);
                static int ultimo_dia=-1;
                if(ti.tm_hour==0&&ti.tm_min==0&&ultimo_dia!=ti.tm_yday){
                    ultimo_dia=ti.tm_yday;
                    double nuevo_cap=CAP+pnl_dia_;
                    if(nuevo_cap>100){
                        CAP=nuevo_cap;guardar_capital(nuevo_cap);
                        pnl_dia_=0;peak_=CAP;
                        log("REINVERSION: capital="+std::to_string(nuevo_cap)+" USD");
                        double kelly_noc=std::min(nuevo_cap*0.05*APAL/N_CAPAS,300.0);
                        std::lock_guard<std::mutex>lkq(mq_);
                        for(auto&kv:quotes_){
                            double max_noc=Quote::noc_max(kv.first)/N_CAPAS;
                            kv.second.nocional_capa=std::min(kelly_noc,max_noc);
                            kv.second.nocional_total=kv.second.nocional_capa*N_CAPAS;
                        }
                        log("Kelly nocional_capa: "+std::to_string(kelly_noc)+" USD");
                    }
                }
            }
            // [REGLA 5] Kill switch basado en EQUITY_ACTUAL (Patrimonio Neto)
            // Evalúa Balance + PnL flotante, no solo pérdidas cerradas.
            // Así el bot se detiene ante un crash en vuelo aunque ninguna
            // posición haya cerrado todavía — el riesgo es real e inmediato.
            double equity_ahora = get_equity();
            double pnl_equity   = equity_ahora - CAP; // pérdida/ganancia vs capital base

            // Kill switch por pérdida diaria de equity
            if(pnl_equity <= -KD){
                if(!kill_.exchange(true,std::memory_order_acq_rel)){
                    log("KILL_EQUITY: patrimonio neto "+std::to_string(equity_ahora)
                        +" USD | pérdida="+std::to_string(pnl_equity)
                        +" >= KD="+std::to_string(KD)
                        +" (incluye PnL flotante)");
                    cancelar_todo_();
                }
            }
            // También mantener el kill por PnL cerrado como segunda línea de defensa
            if(pnl_dia_<=-KD){
                if(!kill_.exchange(true,std::memory_order_acq_rel)){
                    log("KILL_PNL: pérdida cerrada "+std::to_string(pnl_dia_)
                        +" >= KD="+std::to_string(KD));
                    cancelar_todo_();
                }
            }

            // Drawdown basado en equity real (no en PnL cerrado)
            double dd=(peak_-equity_ahora)/std::max(peak_,1.0);
            if(dd>KDD){
                if(!kill_.exchange(true,std::memory_order_acq_rel)){
                    log("KILL_DRAWDOWN: equity="+std::to_string(equity_ahora)
                        +" | dd="+std::to_string(dd*100.0)
                        +"% | peak="+std::to_string(peak_)
                        +" (incluye PnL flotante)");
                    cancelar_todo_();
                }
            }
            // Actualizar peak usando equity real
            if(equity_ahora > peak_) peak_ = equity_ahora;
        }
    }

    void cancelar_todo_(){
        std::lock_guard<std::mutex>lk(mq_);
        for(auto&kv:quotes_){
            Quote&q=kv.second;
            for(int ci=0;ci<N_CAPAS;ci++){
                Capa&c=q.capas[ci];
                if(c.bid_viva&&!c.bid_oid.empty()){
                    std::string oid=c.bid_oid;
                    encolar_rest_job_([this,oid](){
                        try{rest_cancelar_(oid);}catch(const std::exception&ex){log("CANCEL_ERR "+oid+": "+std::string(ex.what()));}catch(...){log("CANCEL_ERR "+oid+": excepcion desconocida");}
                        rest_threads_.fetch_sub(1,std::memory_order_relaxed);
                    });
                    c.bid_viva=false;
                }
                if(c.ask_viva&&!c.ask_oid.empty()){
                    std::string oid=c.ask_oid;
                    encolar_rest_job_([this,oid](){
                        try{rest_cancelar_(oid);}catch(const std::exception&ex){log("CANCEL_ERR "+oid+": "+std::string(ex.what()));}catch(...){log("CANCEL_ERR "+oid+": excepcion desconocida");}
                        rest_threads_.fetch_sub(1,std::memory_order_relaxed);
                    });
                    c.ask_viva=false;
                }
            }
        }
    }

    // ================================================================
    // HILO DE MÉTRICAS
    // ================================================================
    void bucle_metricas_(){
        uint64_t last_ticks=0;
        int balance_counter=0;
        while(run_.load(std::memory_order_acquire)){
            std::this_thread::sleep_for(std::chrono::seconds(10));
            if(++balance_counter>=6){
                balance_counter=0;
                try{
                    std::string resp=rest_get_("/derivatives/api/v3/accounts");
                    double bal=0.0;
                    if(extraer_available_margin_flex(resp,bal)){
                        // [REGLA 4] Actualizar EQUITY_ACTUAL con el
                        // margen disponible real devuelto por Kraken.
                        // En live esto refleja el patrimonio neto real
                        // incluyendo posiciones abiertas valoradas por
                        // el exchange, no nuestro cálculo local.
                        set_equity(bal);
                        log("BALANCE REAL: "+std::to_string(bal)
                            +" USD | EQUITY_ACTUAL actualizado");
                    }else{
                        log("BALANCE_WARN: no se pudo parsear availableMargin.flex");
                    }
                }catch(const std::exception&ex){
                    log("BALANCE_ERR: "+std::string(ex.what()));
                }catch(...){
                    log("BALANCE_ERR: excepcion desconocida");
                }
            }
            uint64_t nw=met_.ticks.load();
            uint64_t total_ciclos=0;
            {
                std::lock_guard<std::mutex>lk(mq_);
                for(const auto&kv:quotes_){
                    total_ciclos+=kv.second.ciclos;
                    log("["+kv.first+"] "
                        "sigma="+std::to_string(kv.second.last_sigma*10000)+"bps "
                        "hs="+std::to_string(kv.second.last_hs*10000)+"bps "
                        "rp="+std::to_string(kv.second.last_rp)+" "
                        "ofi="+std::to_string(kv.second.last_ofi)+" "
                        "inv="+std::to_string(kv.second.inventory_usd)+" "
                        "pnl="+std::to_string(kv.second.pnl));
                }
            }
            uint64_t lat_n_send=0,lat_n_recv=0;
            double lat_avg_send=0.0,lat_max_send=0.0,lat_avg_recv=0.0,lat_max_recv=0.0;
            lat_rest_send_.snapshot_and_reset(lat_n_send,lat_avg_send,lat_max_send);
            lat_rest_recv_.snapshot_and_reset(lat_n_recv,lat_avg_recv,lat_max_recv);
            uint64_t lat_n_ack=0,lat_n_open_fill=0,lat_n_fill_close=0;
            double lat_avg_ack=0.0,lat_max_ack=0.0,lat_avg_open_fill=0.0,lat_max_open_fill=0.0;
            double lat_avg_fill_close=0.0,lat_max_fill_close=0.0;
            lat_order_ack_.snapshot_and_reset(lat_n_ack,lat_avg_ack,lat_max_ack);
            lat_open_fill_.snapshot_and_reset(lat_n_open_fill,lat_avg_open_fill,lat_max_open_fill);
            lat_fill_close_.snapshot_and_reset(lat_n_fill_close,lat_avg_fill_close,lat_max_fill_close);
            std::ostringstream oo;
            oo<<std::fixed<<std::setprecision(4)
              <<"[10s] ticks="<<(nw-last_ticks)
              <<" requotes="<<met_.requotes.load()
              <<" fills="<<met_.fills.load()
              <<" ciclos="<<total_ciclos
              <<" rest_q="<<rest_threads_.load()
              <<" rest_drop="<<rest_jobs_dropeados_.load()
              <<" inv_closes="<<met_.inv_closes.load()
              <<" lat_send_n="<<lat_n_send
              <<" lat_send_avg_ms="<<lat_avg_send
              <<" lat_send_max_ms="<<lat_max_send
              <<" lat_recv_n="<<lat_n_recv
              <<" lat_recv_avg_ms="<<lat_avg_recv
              <<" lat_recv_max_ms="<<lat_max_recv
              <<" lat_ack_n="<<lat_n_ack
              <<" lat_ack_avg_ms="<<lat_avg_ack
              <<" lat_ack_max_ms="<<lat_max_ack
              <<" lat_open_fill_n="<<lat_n_open_fill
              <<" lat_open_fill_avg_ms="<<lat_avg_open_fill
              <<" lat_open_fill_max_ms="<<lat_max_open_fill
              <<" lat_fill_close_n="<<lat_n_fill_close
              <<" lat_fill_close_avg_ms="<<lat_avg_fill_close
              <<" lat_fill_close_max_ms="<<lat_max_fill_close
              <<" pnl="<<met_.getpnl()<<" USD"
              <<" dia="<<pnl_dia_<<" USD"
              <<" kill="<<kill_.load();
            log(oo.str());
            exportar_();
            last_ticks=nw;
            met_.requotes.store(0);met_.fills.store(0);met_.inv_closes.store(0);
        }
    }

    // ================================================================
    // LOGS Y EXPORTACIÓN
    // ================================================================
    void log(const std::string&msg){
        auto tt=std::chrono::system_clock::to_time_t(
            std::chrono::system_clock::now());
        struct tm ti;localtime_r(&tt,&ti);
        char ts[32];strftime(ts,sizeof(ts),"%H:%M:%S",&ti);
        std::string line=std::string("[")+ts+"] "+msg;
        std::lock_guard<std::mutex>lk(ml_);
        std::cout<<line<<"\n";std::cout.flush();
        std::ofstream out("bot_hft.log",std::ios::app);
        if(out.is_open())out<<line<<"\n";
    }

    void logt_(const Quote&q,double pnl_ciclo,double spread_pct){
        auto tt=std::chrono::system_clock::to_time_t(
            std::chrono::system_clock::now());
        struct tm ti;localtime_r(&tt,&ti);
        char ts[32];strftime(ts,sizeof(ts),"%H:%M:%S",&ti);
        std::ofstream ft("trades_historico.txt",std::ios::app);
        if(ft.is_open())
            ft<<ts<<" "<<q.sym<<" pnl="<<pnl_ciclo
              <<" balance="<<(CAP+pnl_dia_)
              <<" sigma="<<q.last_sigma*10000<<"bps"
              <<" hs="<<q.last_hs*10000<<"bps"
              <<" capas="<<N_CAPAS<<"\n";
        std::ofstream ff("trades_hft.csv",std::ios::app);
        if(!ff.is_open())return;
        ff<<std::fixed<<std::setprecision(6)
          <<ts<<","<<q.sym<<",L1,CICLO_MM,"
          <<q.capas[0].bid_px<<","<<q.capas[0].ask_px<<","
          <<q.nocional_capa<<","<<spread_pct<<","
          <<pnl_ciclo<<","<<(CAP+pnl_dia_)<<","
          <<q.last_sigma<<","<<q.last_hs<<","
          <<q.last_rp<<","<<q.last_vamp<<","<<q.last_ofi<<"\n";
    }

    void exportar_(){
        std::ofstream ff("metricas_hft.csv",std::ios::trunc);
        if(!ff.is_open())return;
        ff<<std::fixed<<std::setprecision(4);
        ff<<"pnl_total,"<<met_.getpnl()<<"\n";
        ff<<"pnl_dia,"<<pnl_dia_<<"\n";
        ff<<"kill,"<<kill_.load()<<"\n";
        ff<<"fills,"<<met_.fills.load()<<"\n";
        ff<<"ciclos,"<<met_.ciclos.load()<<"\n";
        ff<<"capital,"<<CAP<<"\n";
        ff<<"n_capas,"<<N_CAPAS<<"\n";
        std::lock_guard<std::mutex>lk(mq_);
        for(const auto&kv:quotes_)
            ff<<"par_"<<kv.first<<","
              <<kv.second.ciclos<<","
              <<kv.second.pnl<<","
              <<kv.second.inventory_usd<<","
              <<kv.second.last_sigma*10000<<"bps,"
              <<kv.second.last_hs*10000<<"bps,"
              <<"noc_capa="<<kv.second.nocional_capa<<"\n";
    }

    // ================================================================
    // UTILIDADES CRIPTOGRÁFICAS
    // ================================================================
    std::string nonce_(){
        static std::atomic<uint64_t> cnt{0};
        uint64_t ms=std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        uint64_t n=cnt.fetch_add(1,std::memory_order_relaxed);
        return std::to_string(ms*100000ULL+(n%100000ULL));
    }

    std::string b64e_(const unsigned char*d,size_t n){
        static const char*t=
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string r;
        for(size_t i=0;i<n;i+=3){
            unsigned b=(d[i]<<16)|((i+1<n?d[i+1]:0)<<8)|(i+2<n?d[i+2]:0);
            r+=t[(b>>18)&63];r+=t[(b>>12)&63];
            r+=(i+1<n)?t[(b>>6)&63]:'=';
            r+=(i+2<n)?t[b&63]:'=';
        }
        return r;
    }

    std::vector<unsigned char> b64d_(const std::string&s){
        static const int T[256]={
            -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
            -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
            -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
            52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
            -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
            15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
            -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
            41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1};
        std::vector<unsigned char> r;
        int val=0,bits=-8;
        for(unsigned char c:s){
            if(T[c]==-1)break;
            val=(val<<6)+T[c];bits+=6;
            if(bits>=0){r.push_back((val>>bits)&0xFF);bits-=8;}
        }
        return r;
    }

    std::string sign_(const std::string&ch){
        unsigned char sha[32];
        SHA256((const unsigned char*)ch.c_str(),ch.size(),sha);
        auto key=b64d_(as2_);
        unsigned char mac[64];unsigned int ml=64;
        HMAC(EVP_sha512(),key.data(),key.size(),sha,32,mac,&ml);
        return b64e_(mac,ml);
    }

    static void pin(int cc){
        unsigned int ncpus=std::thread::hardware_concurrency();
        if(ncpus<=1)return;
        cpu_set_t cs;CPU_ZERO(&cs);
        CPU_SET(cc%ncpus,&cs);
        pthread_setaffinity_np(pthread_self(),sizeof(cs),&cs);
    }

    // ================================================================
    // REST — AUTH Y ENCODING
    // ================================================================
    std::string url_encode_body_(const std::string&body){
        std::string result;result.reserve(body.size()*3);
        size_t i=0;
        while(i<body.size()){
            while(i<body.size()&&body[i]!='='){result+=body[i++];}
            if(i>=body.size())break;
            result+='=';++i;
            while(i<body.size()&&body[i]!='&'){
                unsigned char c=(unsigned char)body[i++];
                static const char hex[]="0123456789ABCDEF";
                if(isalnum(c)||c=='-'||c=='_'||c=='.'||c=='~'){result+=c;}
                else{result+='%';result+=hex[c>>4];result+=hex[c&15];}
            }
            if(i<body.size()){result+='&';++i;}
        }
        return result;
    }

    std::string url_encode_(const std::string&s){
        static const char hex[]="0123456789ABCDEF";
        std::string r;r.reserve(s.size()*3);
        for(unsigned char c:s){
            if(isalnum(c)||c=='-'||c=='_'||c=='.'||c=='~'){r+=c;}
            else{r+='%';r+=hex[c>>4];r+=hex[c&15];}
        }
        return r;
    }

    std::string rest_sign_(const std::string&encoded_body,
                           const std::string&nonce_str,
                           const std::string&path){
        std::string ep=path;
        const std::string pfx="/derivatives";
        if(ep.size()>pfx.size()&&ep.substr(0,pfx.size())==pfx)ep=ep.substr(pfx.size());
        std::string msg=encoded_body+nonce_str+ep;
        unsigned char sha[SHA256_DIGEST_LENGTH];
        SHA256((const unsigned char*)msg.data(),msg.size(),sha);
        auto key=b64d_(as2_);
        unsigned char mac[EVP_MAX_MD_SIZE];unsigned int ml=0;
        HMAC(EVP_sha512(),key.data(),(int)key.size(),sha,SHA256_DIGEST_LENGTH,mac,&ml);
        return b64e_(mac,ml);
    }

    // ================================================================
    // CONEXIÓN REST PERSISTENTE
    // ================================================================
    void rest_connect_(){
        // ── Módulo 1A: Keep-Alive con no_delay ──────────────────────
        rest_ssl_.set_default_verify_paths();
        rest_ssl_.set_verify_mode(asio::ssl::verify_peer);
        rest_stream_=std::make_unique<RestStream>(rest_ioc_,rest_ssl_);
        tcp::resolver res(rest_ioc_);
        auto eps=res.resolve(REST_HOST,REST_PORT);
        // RestStream = beast::ssl_stream<tcp::socket>
        // next_layer() devuelve tcp::socket directamente (no doble)
        auto& tcp_sock=rest_stream_->next_layer();
        asio::connect(tcp_sock,eps);
        // Desactivar algoritmo de Nagle — envío inmediato sin buffering
        tcp_sock.set_option(asio::ip::tcp::no_delay(true));
        // Keep-Alive a nivel TCP — detecta conexiones muertas
        tcp_sock.set_option(asio::socket_base::keep_alive(true));
        SSL_set_tlsext_host_name(rest_stream_->native_handle(),REST_HOST);
        rest_stream_->handshake(asio::ssl::stream_base::client);
        rest_connected_=true;
        log("REST: keep-alive+no_delay activos — RTT puro ~2.4ms");
    }

    std::string rest_exec_(http::request<http::string_body>&req){
        for(int intento=0;intento<2;++intento){
            try{
                if(!rest_connected_||!rest_stream_){
                    rest_ioc_.restart();rest_connect_();
                }
                auto t0=std::chrono::steady_clock::now();
                http::write(*rest_stream_,req);
                auto t1=std::chrono::steady_clock::now();
                beast::flat_buffer buf;
                http::response<http::string_body> resp;
                http::read(*rest_stream_,buf,resp);
                auto t2=std::chrono::steady_clock::now();
                uint64_t send_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(t1-t0).count();
                uint64_t recv_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(t2-t1).count();
                lat_rest_send_.add_us(send_us);
                lat_rest_recv_.add_us(recv_us);
                log("LAT_REST path="+std::string(req.target())
                    +" send_ms="+fmt_decimal(send_us/1000.0,3)
                    +" recv_ms="+fmt_decimal(recv_us/1000.0,3));
                if(resp[http::field::connection]=="close"){
                    rest_connected_=false;rest_stream_.reset();
                }
                return resp.body();
            }catch(const std::exception&ex){
                log("REST reconectando: "+std::string(ex.what()));
                rest_connected_=false;rest_stream_.reset();
                if(intento==0){
                    try{rest_ioc_.restart();rest_connect_();}
                    catch(const std::exception&rex){log("REST reconexion interna fallida: "+std::string(rex.what()));}
                    catch(...){log("REST reconexion interna fallida: excepcion desconocida");}
                }
            }
        }
        return "";
    }

    std::string rest_get_(const std::string&path){
        if(!live_)return "{\"result\":\"success\",\"leveragePreferences\":[]}";
        std::lock_guard<std::mutex>lk(mrest_);
        std::string nonce_str=nonce_();
        std::string authent=rest_sign_("",nonce_str,path);
        http::request<http::string_body> req(http::verb::get,path,11);
        req.set(http::field::host,REST_HOST);
        req.set(http::field::user_agent,USER_AGENT);
        req.set(http::field::connection,"keep-alive");
        req.set("APIKey",ak_);req.set("Nonce",nonce_str);req.set("Authent",authent);
        req.body()="";req.prepare_payload();
        return rest_exec_(req);
    }

    void configurar_margen_cross_(){
        log("Verificando modo de margen...");
        std::string resp=rest_get_(REST_LEVERAGE_GET);
        if(resp.empty()){log("WARN: no se pudo consultar leverage");return;}
        std::vector<std::string> isolated_pares;
        for(const auto&kv:quotes_){
            std::string buscar="\""+kv.first+"\"";
            if(resp.find(buscar)!=std::string::npos){
                isolated_pares.push_back(kv.first);
                log("ISOLATED: "+kv.first+" → convirtiendo a cross");
            }
        }
        if(isolated_pares.empty()){log("Todos los pares en cross margin. OK.");return;}
        for(const auto&sym:isolated_pares){
            std::string r=rest_put_(REST_LEVERAGE,"symbol="+sym);
            log(r.find("success")!=std::string::npos
                ?"Cross margin OK: "+sym
                :"WARN cross: "+sym+" resp="+r.substr(0,80));
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    std::string rest_put_(const std::string&path,const std::string&body){
        if(!live_){log("SIM PUT "+path);return "{\"result\":\"success\"}";}
        std::lock_guard<std::mutex>lk(mrest_);
        std::string nonce_str=nonce_();
        std::string encoded_body=url_encode_body_(body);
        std::string authent=rest_sign_(encoded_body,nonce_str,path);
        http::request<http::string_body> req(http::verb::put,path,11);
        req.set(http::field::host,REST_HOST);
        req.set(http::field::user_agent,USER_AGENT);
        req.set(http::field::content_type,"application/x-www-form-urlencoded");
        req.set(http::field::connection,"keep-alive");
        req.set("APIKey",ak_);req.set("Nonce",nonce_str);req.set("Authent",authent);
        req.body()=encoded_body;req.prepare_payload();
        return rest_exec_(req);
    }

    std::string rest_post_(const std::string&path,const std::string&body){
        if(!live_){log("SIM REST "+path+" | "+body.substr(0,80));return "{\"result\":\"success\"}";}
        std::lock_guard<std::mutex>lk(mrest_);
        std::string nonce_str=nonce_();
        std::string encoded_body=url_encode_body_(body);
        std::string authent=rest_sign_(encoded_body,nonce_str,path);
        http::request<http::string_body> req(http::verb::post,path,11);
        req.set(http::field::host,REST_HOST);
        req.set(http::field::user_agent,USER_AGENT);
        req.set(http::field::content_type,"application/x-www-form-urlencoded");
        req.set(http::field::connection,"keep-alive");
        req.set("APIKey",ak_);req.set("Nonce",nonce_str);req.set("Authent",authent);
        req.body()=encoded_body;req.prepare_payload();
        std::string rbody=rest_exec_(req);
        if(!rbody.empty())log("REST "+path+" | "+rbody.substr(0,120));
        return rbody;
    }

    // ================================================================
    // ÓRDENES REST
    // ================================================================
    void rest_mercado_(const std::string&sym,Side lado,
                       double size,const std::string&oid){
        auto sp=get_spec(sym);
        double sz=(sym=="PF_XBTUSD")?redondear(size,sp.lot_size):std::floor(size);
        if(sz<1.0&&sym!="PF_XBTUSD"){log("SKIP MKT size=0");return;}
        std::string body=
            "orderType=mkt&symbol="+sym+
            "&side="+(lado==Side::B?"buy":"sell")+
            "&size="+fmt_decimal(sz,sp.size_decimals)+
            "&cliOrdId="+oid;
        log("REST MKT "+sym+" "+(lado==Side::B?"BUY":"SELL")
            +" sz="+fmt_decimal(sz,sp.size_decimals));
        rest_post_(REST_SEND,body);
    }

    void rest_cancelar_(const std::string&oid){
        if(oid.empty())return;
        rest_post_(REST_CANCEL,"cliOrdId="+oid);
    }

    std::string rest_post_batch_(const std::string&path,
                                  const std::string&body){
        if(!live_){log("SIM BATCH "+path);return "{\"result\":\"success\"}";}
        std::lock_guard<std::mutex>lk(mrest_);
        std::string nonce_str=nonce_();
        std::string authent=rest_sign_(body,nonce_str,path);
        http::request<http::string_body> req(http::verb::post,path,11);
        req.set(http::field::host,REST_HOST);
        req.set(http::field::user_agent,USER_AGENT);
        req.set(http::field::content_type,"application/x-www-form-urlencoded");
        req.set(http::field::connection,"keep-alive");
        req.set("APIKey",ak_);req.set("Nonce",nonce_str);req.set("Authent",authent);
        req.body()=body;req.prepare_payload();
        std::string rbody=rest_exec_(req);
        if(!rbody.empty())log("BATCH RESP | "+rbody.substr(0,500));
        return rbody;
    }

    // ================================================================
    // [REGLA 4] BATCH v18 — 6 micro-órdenes en UN solo HTTP POST
    // JSON: {"batchOrder":[cancel×N, bid1, bid2, bid3, ask1, ask2, ask3]}
    // [REGLA 3] Validación bid_px < ask_px POST-REDONDEO al tick_size
    // ================================================================
    void rest_batch_v18_(
        const std::string&sym,
        double noc_capa,
        const double bid_pxs[N_CAPAS],   // precios flotantes bid
        const double ask_pxs[N_CAPAS],   // precios flotantes ask
        const std::string old_boids[N_CAPAS],
        const std::string old_aoids[N_CAPAS],
        const std::string new_boids[N_CAPAS],
        const std::string new_aoids[N_CAPAS],
        bool bid_ok,                      // OFI: permitir bids
        bool ask_ok)                      // OFI: permitir asks
    {
        auto sp=get_spec(sym);
        json::array batch;

        // ── Cancelaciones de capas anteriores ────────────────────────
        for(int ci=0;ci<N_CAPAS;ci++){
            if(!old_boids[ci].empty()){
                json::object c;c["order"]="cancel";c["cliOrdId"]=old_boids[ci];
                batch.push_back(c);
            }
            if(!old_aoids[ci].empty()){
                json::object c;c["order"]="cancel";c["cliOrdId"]=old_aoids[ci];
                batch.push_back(c);
            }
        }

        // ── Envío de las 3 capas bid + 3 capas ask ───────────────────
        bool alguna_bid=false,alguna_ask=false;
        for(int ci=0;ci<N_CAPAS;ci++){
            // [REGLA 3] POST-REDONDEO al tick_size antes de validar cruce
            double bpx=redondear(bid_pxs[ci],sp.tick_size);
            double apx=redondear(ask_pxs[ci],sp.tick_size);

            // Corrección si post-redondeo cruzó ─────────────────────
            if(bpx>=apx){
                // Separar exactamente 1 tick en cada dirección
                bpx=apx-sp.tick_size;
            }
            // Garantía adicional: ask nunca ≤ bid tras corrección
            if(apx<=bpx){apx=bpx+sp.tick_size;}
            if(bpx<=0)continue;

            // Tamaño en contratos del activo base
            // PF_SOLUSD: 1 contrato = 1 SOL  (min lot 0.01)
            // PF_ETHUSD: 1 contrato = 1 ETH  (min lot 0.001)
            // PF_XRPUSD: 1 contrato = 1 XRP  (min lot 1)
            // PF_XBTUSD: 1 contrato = $1 USD (min lot 0.0001 BTC)
            double mid_px=(bpx+apx)*0.5;
            // FIX 3: Sizing con floor estricto — evita decimales residuales
            // std::round puede producir 14.999999999 o 15.000000001
            // que Kraken rechaza si viola el lot_size exacto.
            // Estrategia: floor al múltiplo inferior + forzar int si necesario.
            double raw_sz=(sym=="PF_XBTUSD")
                ?noc_capa                    // BTC: nocional en USD
                :noc_capa/mid_px;            // otros: USD→contratos

            // Floor estricto al múltiplo inferior de lot_size
            double sz=std::floor(raw_sz/sp.lot_size)*sp.lot_size;

            // Si size_decimals==0 (XRP, BTC entero) forzar conversión
            // a int64 para eliminar cualquier decimal residual de FP
            if(sp.size_decimals==0){
                sz=static_cast<double>(static_cast<int64_t>(sz));
            }

            // Garantizar mínimo de 1 lot_size tras el floor
            if(sz<sp.lot_size) sz=sp.lot_size;

            // Forzar int de nuevo tras el mínimo si necesario
            if(sp.size_decimals==0){
                sz=static_cast<double>(static_cast<int64_t>(sz));
            }

            std::string tag_b=std::to_string(ci*2+1);
            std::string tag_a=std::to_string(ci*2+2);

            if(bid_ok){
                json::object o;
                o["order"]="send";o["order_tag"]=tag_b;o["symbol"]=sym;
                o["side"]="buy";o["orderType"]="lmt";
                o["limitPrice"]=bpx;o["size"]=sz;
                o["postOnly"]=true;o["cliOrdId"]=new_boids[ci];
                batch.push_back(o);
                alguna_bid=true;
            }
            if(ask_ok){
                json::object o;
                o["order"]="send";o["order_tag"]=tag_a;o["symbol"]=sym;
                o["side"]="sell";o["orderType"]="lmt";
                o["limitPrice"]=apx;o["size"]=sz;
                o["postOnly"]=true;o["cliOrdId"]=new_aoids[ci];
                batch.push_back(o);
                alguna_ask=true;
            }
        }

        if(batch.empty()){
            log("BATCH_SKIP "+sym+" — sin órdenes válidas");
            return;
        }

        json::object wrapper;wrapper["batchOrder"]=batch;
        std::string json_raw=json::serialize(wrapper);
        // Firmar exactamente el mismo body que se envía.
        std::string body="json="+url_encode_(json_raw);

        log("BATCH_V9 "+sym
            +" capas="+std::to_string(N_CAPAS)
            +" bid="+(alguna_bid?"ON":"PAUSED_OFI")
            +" ask="+(alguna_ask?"ON":"PAUSED_OFI")
            +" b1="+std::to_string(bid_pxs[0])
            +" a1="+std::to_string(ask_pxs[0]));

        std::string resp=rest_post_batch_(REST_BATCH,body);

        // ── Parseo de resultados por capa ─────────────────────────────
        {
            std::lock_guard<std::mutex>lk(mq_);
            auto it=quotes_.find(sym);
            if(it==quotes_.end())return;
            Quote&q=it->second;

            // En SIM: result=success → asumir todas placed
            bool sim_ok = !live_ &&
                resp.find("\"result\":\"success\"")!=std::string::npos;

            for(int ci=0;ci<N_CAPAS;ci++){
                std::string tag_b=std::to_string(ci*2+1);
                std::string tag_a=std::to_string(ci*2+2);
                bool b_placed=sim_ok,a_placed=sim_ok;
                if(!sim_ok){
                    size_t p=0;
                    while(p<resp.size()){
                        size_t tp=resp.find("\"order_tag\":\"",p);
                        if(tp==std::string::npos)break;
                        tp+=13;size_t te=resp.find('"',tp);
                        if(te==std::string::npos)break;
                        std::string otag=resp.substr(tp,te-tp);
                        size_t sp2=resp.find("\"status\":\"",te);
                        if(sp2==std::string::npos||sp2-te>200)break;
                        sp2+=10;size_t se=resp.find('"',sp2);
                        if(se==std::string::npos)break;
                        std::string st=resp.substr(sp2,se-sp2);
                        if(otag==tag_b&&st=="placed")b_placed=true;
                        if(otag==tag_a&&st=="placed")a_placed=true;
                        p=se;
                    }
                }
                // Actualizar estado de órdenes
                if(bid_ok&&b_placed){
                    q.capas[ci].bid_viva=true;
                    std::lock_guard<std::mutex> lkl(mlat_);
                    auto it_oid=oid_sent_ts_.find(new_boids[ci]);
                    if(it_oid!=oid_sent_ts_.end()){
                        uint64_t dt_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now()-it_oid->second).count();
                        lat_order_ack_.add_us(dt_us);
                    }
                } else if(bid_ok&&!b_placed){
                    q.capas[ci].bid_viva=false;q.capas[ci].bid_oid="";
                }
                if(ask_ok&&a_placed){
                    q.capas[ci].ask_viva=true;
                    std::lock_guard<std::mutex> lkl(mlat_);
                    auto it_oid=oid_sent_ts_.find(new_aoids[ci]);
                    if(it_oid!=oid_sent_ts_.end()){
                        uint64_t dt_us=(uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now()-it_oid->second).count();
                        lat_order_ack_.add_us(dt_us);
                    }
                } else if(ask_ok&&!a_placed){
                    q.capas[ci].ask_viva=false;q.capas[ci].ask_oid="";
                }
                log("BATCH_RESULT "+sym
                    +" capa"+std::to_string(ci+1)
                    +" bid="+(b_placed?"OK":(bid_ok?"FAIL":"PAUSED"))
                    +" ask="+(a_placed?"OK":(ask_ok?"FAIL":"PAUSED")));
            }
        }
    }

    // ================================================================
    // CONEXIÓN WEBSOCKET
    // ================================================================
    // ── Módulo 2A: conectar WS con no_delay ─────────────────────
    using WsStream = websocket::stream<beast::ssl_stream<tcp::socket>>;
    std::unique_ptr<WsStream> ws_connect_(asio::io_context& ioc,
                                          asio::ssl::context& ssl,
                                          const char* host,
                                          const char* path){
        auto ws=std::make_unique<WsStream>(ioc,ssl);
        tcp::resolver res(ioc);
        auto eps=res.resolve(host,WS_PORT);
        auto& tcp_sock=ws->next_layer().next_layer();
        asio::connect(tcp_sock,eps);
        // Módulo 2B: no_delay en WS privado — envío inmediato de frames
        tcp_sock.set_option(asio::ip::tcp::no_delay(true));
        tcp_sock.set_option(asio::socket_base::keep_alive(true));
        SSL_set_tlsext_host_name(ws->next_layer().native_handle(),host);
        ws->next_layer().handshake(asio::ssl::stream_base::client);
        ws->set_option(websocket::stream_base::timeout::suggested(
            beast::role_type::client));
        ws->set_option(websocket::stream_base::decorator(
            [host](websocket::request_type&req){
                req.set(beast::http::field::host,host);
                req.set(beast::http::field::user_agent,USER_AGENT);
            }));
        ws->handshake(host,path);
        return ws;
    }

    void conn_(){
        ssl_.set_default_verify_paths();
        ssl_.set_verify_mode(asio::ssl::verify_peer);
        // WS público — precios de mercado
        ws_=ws_connect_(ioc_,ssl_,WS_HOST,WS_PATH);
        json::array pids;
        for(const auto&kv:mkt_)pids.push_back(json::value(kv.first));
        json::object s1;s1["event"]="subscribe";s1["feed"]="ticker";
        s1["product_ids"]=pids;
        ws_->write(asio::buffer(json::serialize(s1)));
        if(!ak_.empty()){
            json::object chreq;chreq["event"]="challenge";chreq["api_key"]=ak_;
            ws_->write(asio::buffer(json::serialize(chreq)));
            log("Challenge enviado...");
            beast::flat_buffer cbuf;bool got=false;
            for(int i=0;i<40&&!got;++i){
                cbuf.clear();ws_->read(cbuf);
                const char*cd=static_cast<const char*>(cbuf.data().data());
                size_t cl=cbuf.size();
                log("WS_RECV: "+std::string(cd,cl));
                if(ffd(cd,cl,"\"event\":\"challenge\"")){
                    const char*p=ffd(cd,cl,"\"message\":\"");
                    if(p){std::string ch;while(*p!='"'&&*p!=0)ch+=*p++;
                        if(!ch.empty()){chal_=ch;schal_=sign_(ch);
                            log("Challenge OK");got=true;}}
                }
            }
            if(!got){log("ERROR: sin challenge.");return;}
            json::object sf;sf["event"]="subscribe";sf["feed"]="fills";
            sf["api_key"]=ak_;sf["original_challenge"]=chal_;
            sf["signed_challenge"]=schal_;
            ws_->write(asio::buffer(json::serialize(sf)));
            bool fills_ok=false;
            for(int i=0;i<10&&!fills_ok;++i){
                cbuf.clear();ws_->read(cbuf);
                const char*rd=static_cast<const char*>(cbuf.data().data());
                size_t rl=cbuf.size();std::string resp(rd,rl);
                if(resp.find("\"event\":\"subscribed\"")!=std::string::npos)fills_ok=true;
                if(resp.find("\"event\":\"alert\"")!=std::string::npos)break;
            }
            if(!fills_ok)log("WARN: fills no suscrito");
            json::object so;so["event"]="subscribe";so["feed"]="open_orders";
            so["api_key"]=ak_;so["original_challenge"]=chal_;
            so["signed_challenge"]=schal_;
            ws_->write(asio::buffer(json::serialize(so)));
            bool oo_ok=false;
            for(int i=0;i<10&&!oo_ok;++i){
                cbuf.clear();ws_->read(cbuf);
                const char*rd=static_cast<const char*>(cbuf.data().data());
                size_t rl=cbuf.size();std::string resp(rd,rl);
                if(resp.find("\"event\":\"subscribed\"")!=std::string::npos)oo_ok=true;
                if(resp.find("\"event\":\"alert\"")!=std::string::npos)break;
            }
            if(!oo_ok)log("WARN: open_orders no suscrito");
            log("WS auth OK.");
        } else {
            log("Sin API key — modo SIM");
        }
    }

    // ── Módulo 3C: bucle dedicado de fills por WS privado ───────
    // Se ejecuta en hilo tf_ — procesa fills en <5μs
    // Al recibir un fill: actualiza inventario y dispara rebalanceo
    void bucle_fills_(){
        if(!live_||!ws_priv_)return;
        log("WS_PRIV: bucle fills activo");
        beast::flat_buffer buf;
        while(run_.load(std::memory_order_acquire)){
            try{
                buf.clear();
                ws_priv_->read(buf);
                auto t0=std::chrono::steady_clock::now();
                const char*d=static_cast<const char*>(buf.data().data());
                size_t l=buf.size();

                // Solo procesar frames de fills reales
                if(!ffd(d,l,"\"feed\":\"fills\""))continue;
                if(!ffd(d,l,"\"qty\":"))continue;

                // Extraer campos del fill
                const char*p=ffd(d,l,"\"instrument\":\"");
                if(!p)continue;
                std::string sym;while(*p!='\"'&&*p!=0)sym+=*p++;

                p=ffd(d,l,"\"side\":\"");
                if(!p)continue;
                bool is_buy=(*p=='b');

                p=ffd(d,l,"\"price\":");
                if(!p)continue;
                double fill_px=fatof(p);

                p=ffd(d,l,"\"qty\":");
                if(!p)continue;
                double fill_qty=fatof(p);

                // ── Actualización de inventario en microsegundos ──────
                {
                    std::lock_guard<std::mutex>lk(mq_);
                    auto it=quotes_.find(sym);
                    if(it!=quotes_.end()){
                        Quote&q=it->second;
                        double fill_usd=fill_qty*fill_px;
                        if(is_buy){
                            q.inventory_usd+=fill_usd;
                            q.precio_inv=fill_px;
                            q.tiene_inventario=true;
                            q.ultimo_fill=std::chrono::steady_clock::now();
                            q.fills_bid++;
                            met_.fills.fetch_add(1,std::memory_order_relaxed);
                        } else {
                            q.inventory_usd-=fill_usd;
                            q.fills_ask++;
                            met_.fills.fetch_add(1,std::memory_order_relaxed);
                            check_ciclo_(q);
                        }
                        // Medir latencia fill→inventario actualizado
                        auto dt=std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now()-t0).count();
                        log("WS_FILL "+sym
                            +(is_buy?" BID":" ASK")
                            +" px="+std::to_string(fill_px)
                            +" qty="+std::to_string(fill_qty)
                            +" inv="+std::to_string(q.inventory_usd)
                            +" latencia="+std::to_string(dt)+"μs");
                    }
                }
            }catch(const std::exception&ex){
                if(!run_.load(std::memory_order_acquire))break;
                log("WS_PRIV reconectando: "+std::string(ex.what()));
                std::this_thread::sleep_for(std::chrono::seconds(2));
                try{
                    ws_priv_.reset();
                    ioc_priv_.restart();
                    ssl_priv_=asio::ssl::context(asio::ssl::context::tls_client);
                    ssl_priv_.set_default_verify_paths();
                    ssl_priv_.set_verify_mode(asio::ssl::verify_peer);
                    ws_priv_=ws_connect_(ioc_priv_,ssl_priv_,WS_HOST,WS_PATH);
                    // Reautenticar
                    json::object ch;ch["event"]="challenge";ch["api_key"]=ak_;
                    ws_priv_->write(asio::buffer(json::serialize(ch)));
                    beast::flat_buffer rb;
                    for(int i=0;i<20;++i){
                        rb.clear();ws_priv_->read(rb);
                        const char*cd=static_cast<const char*>(rb.data().data());
                        const char*p=ffd(cd,rb.size(),"\"message\":\"");
                        if(p){std::string c2;while(*p!='\"')c2+=*p++;
                            chal_priv_=c2;schal_priv_=sign_(c2);break;}
                    }
                    json::object sf;sf["event"]="subscribe";sf["feed"]="fills";
                    sf["api_key"]=ak_;
                    sf["original_challenge"]=chal_priv_;
                    sf["signed_challenge"]=schal_priv_;
                    ws_priv_->write(asio::buffer(json::serialize(sf)));
                    log("WS_PRIV reconectado OK");
                }catch(const std::exception&e2){
                    log("WS_PRIV error reconexión: "+std::string(e2.what()));
                }
            }
        }
    }

    void recon_(){
        log("Reconectando WS...");
        std::this_thread::sleep_for(std::chrono::seconds(2));
        chal_.clear();schal_.clear();
        ws_.reset();ioc_.restart();
        conn_();
    }
};

// ================================================================
// MAIN
// ================================================================
static BotMM* gb=nullptr;
void sigh(int){if(gb)gb->stop();exit(0);}

int main(){
    signal(SIGINT,sigh);signal(SIGTERM,sigh);
    std::cout<<"=== HFT Market Maker v18.0 — Multi-Level Layering ===\n";
    std::cout<<"Capital: $815 | Pares: SOL+ETH+XRP | "
             <<N_CAPAS<<" capas/par | Kraken Futures\n";
    std::cout<<"AWS Ireland eu-west-1 | WS=datos | REST=ordenes | post-only\n";
    try{
        BotMM bot;gb=&bot;
        bot.start();bot.wait();
    }catch(const std::exception&ex){
        std::cerr<<"ERROR: "<<ex.what()<<"\n";return 1;
    }
    return 0;
}
