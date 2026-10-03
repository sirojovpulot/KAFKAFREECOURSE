# Apache Kafka kursi 2026: noldan professional darajagacha bepul kurs o'zbek tilida

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-4.3-231F20?logo=apachekafka&logoColor=white" alt="Apache Kafka 4.3">
  <img src="https://img.shields.io/badge/KRaft-ZooKeepersiz-blue" alt="KRaft ZooKeepersiz">
  <img src="https://img.shields.io/badge/til-o%27zbek-red" alt="Kurs o'zbek tilida">
  <img src="https://img.shields.io/badge/narx-bepul-brightgreen" alt="Bepul kurs">
  <img src="https://img.shields.io/badge/daraja-junior%20→%20senior-orange" alt="Juniordan seniorgacha">
</p>

> **Tarjima haqida.** Bu [justxor](https://github.com/justxor) muallifligidagi [ruscha kursning](https://github.com/justxor/KAFKAFREECOURSR) o'zbekcha tarjimasi. Asl matn: [README.ru.md](README.ru.md). Tarjima asl kursning `1195b09` (2026-09-15) holatiga asoslangan.

> **Apache Kafka bo'yicha o'zbek tilidagi to'liq bepul kurs.** Nazariya, amaliyot, Docker, Java, Go va Python, producer va consumerning ichki tuzilishi, replikatsiya, KRaft, exactly-once, Kafka Streams, Kafka Connect, Schema Registry, monitoring, xavfsizlik, unumdorlikni sozlash (tuning) va production arxitekturasi. Hammasi bitta READMEda, **Kafka 4.x (2026)** uchun dolzarb.

**Kafkani ortiqcha gapsiz o'rganish:** har bir modul tushunarli nazariya, sxemalar, o'zingizda ishga tushirish mumkin bo'lgan buyruqlar, tipik xatolar va o'z-o'zini tekshirish savollaridan iborat. Kurs Kafkani noldan o'rganish, backend, data engineer yoki DevOps lavozimiga ish suhbatiga tayyorlanish va productionda Kafka asosida ishonchli tizim loyihalash uchun mos keladi.

⭐ Agar kurs foydali bo'lsa, repozitoriyga yulduzcha qo'ying: shunda uni boshqa dasturchilar ham topadi.

---

## Bu Kafka kursi kimlar uchun

| Siz kimsiz | Nimaga ega bo'lasiz |
|---|---|
| Xabar brokerlarida **yangi boshlovchi** | Kafka nima ekani, nima uchun kerakligi va uni 5 daqiqada qanday ishga tushirishni tushunish |
| **Backend dasturchi** (Java, Go, Python, Node.js, .NET) | Ishonchli producer va consumer, xatolarni qayta ishlash, idempotentlik, transactional outbox |
| **Data engineer** | Debezium bilan CDC, Kafka Connect, Kafka Streams, ma'lumotlar sxemalari, event streaming pipelinelari |
| **DevOps / SRE** | KRaft klasteri, replikatsiya, monitoring, alertlar, xavfsizlik, tuning, Kubernetes |
| **Arxitektor / Tech Lead** | Topiclarni loyihalash, event-driven architecture, multi-DC, antipatternlar |
| **Ish suhbatiga tayyorlanyapsiz** | Kafka bo'yicha junior, middle va senior darajasidagi 30 ta savol va javoblari |

## Kursdan keyin nimalarni qila olasiz

- Apache Kafka arxitekturasini tushuntirib berish: broker, topic, partition, offset, replica, ISR, controller, KRaft;
- Dockerda uch tugunli (node) Kafka klasterini ko'tarish va leader election jarayonini kuzatib, uni buzib ko'rish;
- Java, Go va Pythonda xabarlarni yo'qotmaydigan va takrorlamaydigan producer va consumer yozish;
- vazifaga qarab `acks`, `min.insync.replicas`, `linger.ms`, `batch.size` va compressionni tanlash;
- at-most-once, at-least-once va exactly-once nima ekanini tushunish va har bir semantikani amalga oshirish;
- Kafka tranzaksiyalari va transactional outbox patternidan foydalanish;
- retry-topiclar va DLQ (dead letter queue) qurish;
- hodisalar sxemasini va uning evolyutsiyasini Schema Registry (Avro, Protobuf) orqali loyihalash;
- ma'lumotlar bazalarini Kafka Connect va Debezium (CDC) orqali ulash;
- Kafka Streamsda stream processing yozish;
- Share Groupsdan foydalanish (Kafkadagi navbatlar, KIP-932);
- consumer lag va under-replicated partitionlarni monitoring qilish hamda alertlarni sozlash;
- TLS, SASL/SCRAM, ACL va kvotalarni yoqish;
- production uchun partitionlar, disklar va brokerlar sonini hisoblash.

---

## Mundarija

- [Bu Kafka kursi kimlar uchun](#bu-kafka-kursi-kimlar-uchun)
- [Kursdan keyin nimalarni qila olasiz](#kursdan-keyin-nimalarni-qila-olasiz)
- [Kursni qanday o'tish kerak](#kursni-qanday-otish-kerak)
- [Modul 0. Apache Kafka nima va u nima uchun kerak](#modul-0-apache-kafka-nima-va-u-nima-uchun-kerak)
- [Modul 1. Kafka arxitekturasi: broker, topic, partition, offset](#modul-1-kafka-arxitekturasi-broker-topic-partition-offset)
- [Modul 2. Kafkani Dockerda o'rnatish va birinchi buyruqlar](#modul-2-kafkani-dockerda-ornatish-va-birinchi-buyruqlar)
- [Modul 3. Partitionlar, kalitlar va xabarlar tartibi](#modul-3-partitionlar-kalitlar-va-xabarlar-tartibi)
- [Modul 4. Kafka replikatsiyasi: leader, ISR, acks, min.insync.replicas](#modul-4-kafka-replikatsiyasi-leader-isr-acks-mininsyncreplicas)
- [Modul 5. Kafka Producer: qanday tuzilgan va qanday sozlanadi](#modul-5-kafka-producer-qanday-tuzilgan-va-qanday-sozlanadi)
- [Modul 6. Kafka Consumer va Consumer Groups](#modul-6-kafka-consumer-va-consumer-groups)
- [Modul 7. Exactly-once, Kafka tranzaksiyalari va transactional outbox](#modul-7-exactly-once-kafka-tranzaksiyalari-va-transactional-outbox)
- [Modul 8. Ma'lumotlarni saqlash: segmentlar, retention, log compaction](#modul-8-malumotlarni-saqlash-segmentlar-retention-log-compaction)
- [Modul 9. Share Groups: Kafkadagi navbatlar](#modul-9-share-groups-kafkadagi-navbatlar)
- [Modul 10. Schema Registry, Avro, Protobuf va hodisalarni loyihalash](#modul-10-schema-registry-avro-protobuf-va-hodisalarni-loyihalash)
- [Modul 11. Kafka Connect va Debezium bilan CDC](#modul-11-kafka-connect-va-debezium-bilan-cdc)
- [Modul 12. Kafka Streams: ma'lumotlar oqimini qayta ishlash](#modul-12-kafka-streams-malumotlar-oqimini-qayta-ishlash)
- [Modul 13. Xatolarni qayta ishlash: retry, DLQ va poison pill](#modul-13-xatolarni-qayta-ishlash-retry-dlq-va-poison-pill)
- [Modul 14. Kafka unumdorligi va tuning](#modul-14-kafka-unumdorligi-va-tuning)
- [Modul 15. Kafka monitoringi: metrikalar, consumer lag, alertlar](#modul-15-kafka-monitoringi-metrikalar-consumer-lag-alertlar)
- [Modul 16. Kafka xavfsizligi: TLS, SASL, ACL, kvotalar](#modul-16-kafka-xavfsizligi-tls-sasl-acl-kvotalar)
- [Modul 17. Kafka productionda: arxitektura va ekspluatatsiya](#modul-17-kafka-productionda-arxitektura-va-ekspluatatsiya)
- [Modul 18. Yakuniy loyiha: event-driven internet-do'kon](#modul-18-yakuniy-loyiha-event-driven-internet-dokon)
- [Kafka CLI shpargalkasi](#kafka-cli-shpargalkasi)
- [Muhim sozlamalar shpargalkasi](#muhim-sozlamalar-shpargalkasi)
- [Kafka bo'yicha ish suhbati savollari va javoblari](#kafka-boyicha-ish-suhbati-savollari-va-javoblari)
- [FAQ: Apache Kafka haqida ko'p beriladigan savollar](#faq-apache-kafka-haqida-kop-beriladigan-savollar)
- [Kafka lug'ati](#kafka-lugati)
- [Rasmiy manbalar va keyin nima o'qish kerak](#rasmiy-manbalar-va-keyin-nima-oqish-kerak)

---

## Kursni qanday o'tish kerak

1. **Tartib bilan boring.** 0-4 modullar poydevor. Partition, offset va ISR nima ekanini tushunmasangiz, qolgan hammasi sehrgarlikdek tuyuladi.
2. **Har bir buyruqni ishga tushiring.** Kafka qo'l bilan o'rganiladi. Rebalance haqida o'qish va uni loglarda ko'rish tushunishning turli darajalari.
3. **Klasterni buzing.** Brokerlarni to'xtating, consumerni o'ldiring, diskni to'ldirib yuboring. Production tajribasi aynan shunday paydo bo'ladi.
4. **Modul oxiridagi savollarga** xuddi ish suhbatidagidek ovoz chiqarib **javob bering.**
5. **Yakuniy loyihani bajaring.** U barcha mavzularni bitta tizimga jamlaydi.

**Nimalarni o'rnatish kerak:** Docker va Docker Compose, Java 17+ (Java va Kafka Streams misollari uchun), Git, istalgan IDE. Go misollari uchun Go 1.22+, Python misollari uchun Python 3.10+ kerak.

**Versiya:** barcha misollar Apache Kafka 4.x uchun yozilgan, image `apache/kafka:4.3.1`. Kafka 4.x faqat KRaft rejimida ishlaydi, ZooKeeper butunlay olib tashlangan.

---

# Modul 0. Apache Kafka nima va u nima uchun kerak

## 0.1 Apache Kafkaning sodda tildagi ta'rifi

**Apache Kafka** hodisalarni oqim tarzida uzatuvchi taqsimlangan platforma (event streaming platform). U uchta ishni bajaradi:

1. Hodisalar oqimlarini **e'lon qiladi va ularga obuna bo'ladi** (xabar brokeri kabi).
2. Hodisalarni diskda kerakli muddatgacha **ishonchli saqlaydi**: soatlab, kunlab, yillab.
3. Hodisalar oqimlarini real vaqtda **qayta ishlaydi** yoki tarixni qaytadan o'qiydi.

Kafkaning asosiy g'oyasi: **taqsimlangan append-only log** (faqat oxiriga qo'shib yozish mumkin bo'lgan jurnal). Qolgan hamma narsa, replikatsiyadan tortib exactly-oncegacha, shu oddiy tuzilma atrofida qurilgan.

Kafka 2011-yilda LinkedInda foydalanuvchilar faolligini qayta ishlash uchun yaratilgan, keyin Apache Software Foundationga topshirilgan va bugun Fortune 100 ro'yxatidagi kompaniyalarning ko'pchiligi undan foydalanadi: banklar, marketpleyslar, telekom, taksi, striming servislari, o'yin studiyalari.

## 0.2 Kafka hal qiladigan muammo

Internet-do'konni tasavvur qilaylik. Foydalanuvchi buyurtma berdi va bu haqda birdaniga bir nechta tizim xabar topishi kerak:

```text
                    +--> Payment Service
                    |
Order Service ------+--> Warehouse
                    |
                    +--> Analytics
                    |
                    +--> Notification Service
                    |
                    +--> Fraud Detection
```

Kafkasiz Order Service har bir servisni HTTP orqali to'g'ridan-to'g'ri chaqiradi:

```text
Order Service
   |
   +--> POST /payment
   +--> POST /warehouse
   +--> POST /analytics
   +--> POST /notification
   +--> POST /fraud
```

Kichik loyihada bu ishlaydi. Keyin muammolar boshlanadi:

| Savol | Sinxron integratsiya muammosi |
|---|---|
| Notification Service ishdan chiqdi | Buyurtma butunlay yiqiladi yoki bildirishnoma yo'qoladi |
| Analytics 3 soniya sekinlashyapti | Foydalanuvchi buyurtmani rasmiylashtirishda 3 soniya kutadi |
| Yana 10 ta iste'molchi paydo bo'ldi | Order Serviceni o'zgartirish va deploy qilishga to'g'ri keladi |
| Kechagi buyurtmalarni qayta o'qish kerak | Ma'lumotlar endi hech qayerda saqlanmagan |
| Qora jumadagi yuklama cho'qqisi | Quyi oqimdagi servislar kaskad bo'lib yiqiladi |

Bu **kuchli bog'liqlik (tight coupling)** deyiladi: har bir servis boshqa har bir servis haqida biladi.

## 0.3 Xuddi shu tizim Kafka bilan qanday ko'rinadi

```text
Order Service
     |
     | OrderCreated
     v
+-----------------+
|  Kafka topic    |
|  orders         |
+-----------------+
     |      |      |       |
     v      v      v       v
Payment  Warehouse Analytics Fraud
```

Order Service **bitta hodisa** `OrderCreated` ni e'lon qiladi va boshqa hech kim haqida bilmaydi. Har bir iste'molchi hodisalarni o'z sur'atida o'qiydi.

Nimaga erishdik:

- **Kuchsiz bog'liqlik.** Yangi servis Order Serviceni o'zgartirmasdan ulanadi.
- **Buferlash.** Agar Analytics ishdan chiqsa, hodisalar uni Kafkada kutib turadi va u tiklanganidan keyin qayta ishlanadi.
- **Cho'qqilarni tekislash.** Kafka soniyasiga millionlab hodisani qabul qiladi, iste'molchilar esa ularni o'zlariga qulay tezlikda qayta ishlaydi.
- **Replay.** Hodisalar saqlanayotgan muddat (retention) ichida ularni istalgan davr uchun qayta o'qish mumkin.
- **Masshtablash.** O'qish yuklamasi servis nusxalari (instance) o'rtasida taqsimlanadi.

## 0.4 Hodisa (event) nima

**Hodisa** allaqachon sodir bo'lgan narsa haqidagi o'zgarmas fakt.

```json
{
  "event_id": "5f1c2a7e-9b1d-4c1e-8a4e-0c7b2d9f1a11",
  "event_type": "OrderCreated",
  "event_version": 1,
  "occurred_at": "2026-09-15T10:21:43Z",
  "order_id": "order-123",
  "user_id": "user-42",
  "amount": 4990,
  "currency": "RUB"
}
```

Hodisaning xususiyatlari:

- **o'tgan zamonda** yoziladi: `OrderCreated`, `PaymentFailed`, `UserRegistered`;
- **o'zgarmas**: hodisani tahrirlab bo'lmaydi, faqat yangisini e'lon qilish mumkin (`OrderCancelled`);
- fakt sodir bo'lgan **vaqtni** o'z ichiga oladi;
- iste'molchi dublikatni tashlab yubora olishi uchun **noyob identifikatorga** ega.

### Hodisa va buyruq: bir xil narsa emas

| | Event (hodisa) | Command (buyruq) |
|---|---|---|
| Ma'nosi | Bu allaqachon sodir bo'ldi | Buni bajar |
| Misol | `OrderCreated` | `CreateOrder` |
| Zamon | O'tgan zamon | Buyruq mayli |
| Qabul qiluvchilar | Istalgan sonda, yuboruvchi ularni bilmaydi | Odatda bitta aniq qabul qiluvchi |
| Rad etish mumkinmi | Yo'q, fakt allaqachon sodir bo'lgan | Ha, buyruqni rad etish mumkin |

Kafka hodisalar uchun juda mos keladi. Buyruqlar ham Kafka orqali uzatiladi, lekin bu o'ziga xos murosalari (trade-off) bor alohida integratsiya uslubi.

## 0.5 Kafka shunchaki xabarlar navbati emas

Yangi boshlovchilarning keng tarqalgan xatosi:

```text
Kafka = katta yuklamalar uchun RabbitMQ
```

Klassik navbatda xabar iste'molchi uni olganidan keyin **o'chiriladi**. Kafkada esa xabar **jurnalda qoladi**, iste'molchi faqat qayergacha o'qiganini, ya'ni pozitsiyani (offset) eslab qoladi.

```text
partition-0

offset:  0    1    2    3    4    5
        [A]  [B]  [C]  [D]  [E]  [F]  <-- append (oxiriga yozish)
                        ^
                        |
          consumer group "analytics" offset 3 gacha o'qigan

          consumer group "billing" offset 0 dan mustaqil o'qishi mumkin
```

Kafkaning asosiy xususiyatlari shundan kelib chiqadi:

- bir xil ma'lumotlarning **ko'plab mustaqil o'quvchilari**;
- tarixni **qayta o'qish**;
- partition ichida **qat'iy tartib**;
- diskka ketma-ket yozish hisobiga **ulkan o'tkazuvchanlik**.

## 0.6 Kafka vs RabbitMQ vs HTTP vs Redis Streams

| Mezon | Apache Kafka | RabbitMQ | HTTP/gRPC | Redis Streams |
|---|---|---|---|---|
| Model | Taqsimlangan log | Navbatlar brokeri (AMQP) | So'rov-javob | Xotiradagi log |
| O'qilgandan keyin saqlash | Ha, retention bo'yicha | Yo'q (klassik navbatlar) | Yo'q | Ha, xotira bilan cheklangan |
| Tarixni replay qilish | Ha | Cheklangan (Streams) | Yo'q | Ha |
| O'tkazuvchanlik | Klasterga millionlab msg/s | O'n minglab-yuz minglab msg/s | Servisga bog'liq | Yuqori, lekin RAM bilan cheklangan |
| Tartib | Partition ichida | Navbat ichida | Yo'q | Stream ichida |
| Murakkab marshrutlash | Yo'q, topiclar va iste'molchilar orqali | Ha (exchanges, routing keys) | Yo'q | Yo'q |
| Kechikish | Millisoniyalar | Submillisoniyalar-millisoniyalar | Tarmoqqa bog'liq | Submillisoniyalar |
| Qachon tanlash kerak | Event streaming, analitika, CDC, mikroservislar integratsiyasi | Task queues, RPC, murakkab routing | Foydalanuvchiga sinxron javob | Bitta servis ichidagi yengil streamlar |

> Kafka 4.2 dan boshlab **Share Groups** (Kafkadagi navbatlar, KIP-932) paydo bo'ldi: iste'molchilar bitta partition xabarlarini parallel ravishda, har birini alohida tasdiqlab qayta ishlashi mumkin. Bu RabbitMQ ssenariylarining bir qismini qoplaydi. Batafsil [9-modulda](#modul-9-share-groups-kafkadagi-navbatlar).

### Kafka va HTTP birgalikda

Kafka HTTPning o'rnini bosmaydi. Tipik sxema:

```text
Client --HTTP--> Order API --(buyurtmani saqlaydi, 201 qaytaradi)--> Client
                     |
                     +--event OrderCreated--> Kafka --> boshqa servislar
```

HTTP foydalanuvchi sinxron javob kutayotganda kerak. Kafka hodisalarni asinxron tarqatish uchun kerak.

## 0.7 Kafka qayerda ishlatiladi: real ssenariylar

| Ssenariy | Kafka qanday qo'llanadi |
|---|---|
| **Mikroservislar** | Event-driven o'zaro aloqa, sagalar xoreografiyasi, o'zgarishlarni tarqatish |
| **Real vaqtdagi analitika** | Kliklar, ko'rishlar, ilovalar hodisalari Kafkaga, u yerdan ClickHouse, Druid, Pinot, Snowflakega boradi |
| **Loglar va metrikalarni yig'ish** | Ilovalar loglarni Kafkaga yozadi, undan keyin Elasticsearch/OpenSearch, Loki, S3 |
| **CDC (Change Data Capture)** | Debezium PostgreSQL WALini yoki MySQL binlogini o'qiydi va jadvallardagi o'zgarishlarni e'lon qiladi |
| **Moliya va fintex** | Tranzaksiyalar, antifrod, balanslarni hisoblash, audit |
| **IoT va telemetriya** | Datchiklar, kuryerlar va taksilarning geopozitsiyasi, avtomobillar telemetriyasi |
| **Machine Learning** | Online feature store, oqimli belgilar (streaming features), hodisalarni modellarga yetkazish |
| **Ma'lumotlar integratsiyasi** | Legacy tizimlar, ma'lumotlar ombori va yangi servislar o'rtasidagi markaziy shina |

## 0.8 Kafka qachon kerak emas

Kafka murakkab taqsimlangan tizim. Quyidagi hollarda uni tanlamang:

- sizda bitta monolit va bir-ikkita fon vazifasi bor: ma'lumotlar bazasidagi navbat yoki Redis yetarli;
- foydalanuvchiga sinxron javob kerak: HTTP yoki gRPCdan foydalaning;
- yuklama daqiqasiga yuzlab xabar va replay kerak emas;
- headerlar bo'yicha murakkab marshrutlash, prioritetlar va xabarga TTL kerak: RabbitMQga qarang;
- jamoada klasterni qo'llab-quvvatlashga tayyor odam yo'q va managed Kafka uchun byudjet ham yo'q.

**Kafka haqiqatan foydali bo'ladi, qachonki** xabar yuboruvchilar va iste'molchilar ko'p bo'lsa, hodisalar tarixi kerak bo'lsa, yuklama yuqori bo'lsa, bir nechta jamoa bir xil ma'lumotlar bilan ishlasa, CDC yoki oqimli analitika bo'lsa.

## 0.9 Kafka siz uchun nimalarni QILMAYDI

Kafka infratuzilma darajasidagi kafolatlarni beradi. Tizimning to'g'ri ishlashini baribir o'zingiz loyihalaysiz:

- ilova tomonida qayta ishlashning idempotentligi;
- hodisalar sxemasi va versiyalanishi;
- retry strategiyasi va «zaharli» xabarlarni (poison pill) qayta ishlash;
- monitoring va alertlar;
- xavfsizlik va kirish huquqlarini chegaralash;
- biznes invariantlariga mos partitsiyalash kalitlarini tanlash.

### O'z-o'zini tekshirish uchun savollar

1. Hodisa buyruqdan nimasi bilan farq qiladi?
2. Nima uchun Kafkada xabar o'qilgandan keyin o'chirilmaydi va bu nima beradi?
3. Qaysi hollarda Kafka o'rniga RabbitMQni tanlaysiz?
4. Nima uchun HTTP chaqiruvlarining sinxron zanjiri yuklama cho'qqisiga yomon bardosh beradi?

---

# Modul 1. Kafka arxitekturasi: broker, topic, partition, offset

## 1.1 Asosiy iyerarxiya

```text
Cluster (klaster)
  └── Broker (Kafka serveri)
        └── Topic (hodisalarning mantiqiy oqimi)
              └── Partition (tartiblangan jurnal)
                    └── Segment (diskdagi fayl)
                          └── Record (yozuv)
```

Bu rasmni eslab qoling. Butun kurs shunga tayanadi.

## 1.2 Kafkaning asosiy komponentlari

| Komponent | Bu nima | O'xshatish |
|---|---|---|
| **Record** (message) | Bitta yozuv: key, value, headers, timestamp | Jurnaldagi satr |
| **Topic** | Bir turdagi yozuvlarning nomlangan oqimi | Ma'lumotlar bazasidagi jadval |
| **Partition** | Topic ichidagi tartiblangan, o'zgarmas jurnal | Jadval shardi |
| **Offset** | Partition ichidagi yozuvning tartib raqami | Satr raqami |
| **Broker** | Partitionlarni saqlaydigan va klientlarga xizmat ko'rsatadigan Kafka jarayoni | Ma'lumotlar bazasi serveri |
| **Cluster** | Brokerlar guruhi | Ma'lumotlar bazasi klasteri |
| **Controller** | Klaster metadatasini boshqaradigan tugun (KRaft) | Metadata masteri |
| **Producer** | Yozuvlarni yozadigan klient | Yozuvchi |
| **Consumer** | Yozuvlarni o'qiydigan klient | O'quvchi |
| **Consumer Group** | Partitionlarni o'zaro bo'lib oladigan consumerlar guruhi | Workerlar puli |
| **Replica** | Partitionning boshqa brokerdagi nusxasi | Ma'lumotlar bazasi replikasi |

## 1.3 Yozuv (Record) tuzilishi

```text
+---------------------------------------------------+
| Record                                            |
|---------------------------------------------------|
| key        : "order-123"   (null bo'lishi mumkin) |
| value      : {...json/avro/protobuf...}           |
| headers    : trace-id=abc, source=web             |
| timestamp  : 1789460503000                        |
| offset     : 42  (broker tayinlaydi)              |
| partition  : 3   (producer tanlaydi)              |
+---------------------------------------------------+
```

- **key** partitionni, demak hodisalar tartibini ham belgilaydi. Bir xil kalitli barcha yozuvlar bitta partitionga tushadi.
- **value** foydali yuk (payload). Kafka baytlar bilan ishlaydi va ma'lumotlar formatini bilmaydi.
- **headers** metadata: trace id, hodisa turi, sxema versiyasi.
- **timestamp** `CreateTime` (producerdagi yaratilish vaqti) yoki `LogAppendTime` (brokerga yozilgan vaqt) bo'lishi mumkin, `message.timestamp.type` orqali sozlanadi.

Yozuvlar **batchlar (RecordBatch)** ko'rinishida uzatiladi va saqlanadi. Siqish butun batchga qo'llanadi, shuning uchun u juda samarali.

## 1.4 Topic

**Topic** hodisalar oqimining mantiqiy nomi: `orders`, `payments`, `user-clicks`.

- Topic bitta yoki bir nechta partitiondan iborat.
- Topicning o'z sozlamalari bor: `retention.ms`, `cleanup.policy`, `min.insync.replicas`, `max.message.bytes`.
- Topic xabarni o'qilgandan keyin o'chirmaydi.

Productionda **topiclarni nomlashni** standartlashtirgan ma'qul:

```text
<domen>.<obyekt>.<tur>.<versiya>

shop.orders.events.v1
payments.transactions.events.v1
crm.customers.cdc.v1
shop.orders.events.v1.dlq
```

## 1.5 Partition: masshtablash va tartib birligi

**Partition** alohida tartiblangan jurnal. Har bir partition butunligicha bitta brokerda saqlanadi (bunga qo'shimcha boshqa brokerlarda replikalari bo'ladi).

```text
topic: orders (3 partitions)

partition-0:  [0:order-101] [1:order-104] [2:order-108]
partition-1:  [0:order-102] [1:order-105]
partition-2:  [0:order-103] [1:order-106] [2:order-107] [3:order-109]
```

Partition quyidagilarni belgilaydi:

- **yozish parallelligi**: turli partitionlar turli brokerlarga yoziladi;
- **o'qish parallelligi**: bitta partitionni guruhdagi ko'pi bilan bitta consumer o'qiydi;
- **tartib doirasi**: Kafka tartibni **faqat partition ichida** kafolatlaydi;
- brokerlar o'rtasida **ma'lumotlar taqsimoti**;
- topicning **o'tkazuvchanlik chegarasi**.

Shuning uchun partition topicdan muhimroq. Topic shunchaki nom, tizimning fizikasi esa partitionlarda yashaydi.

## 1.6 Offset

**Offset** yozuvning **bitta partition ichidagi** monoton o'sib boruvchi raqami.

```text
partition-0: offset 0, 1, 2, 3, 4 ...
partition-1: offset 0, 1, 2, 3 ...
```

Muhim oqibatlar:

- offset **topic ichida noyob emas**: offset 5 har bir partitionda bor;
- offset **xabar IDsi emas**: deduplikatsiya uchun o'zingizning `event_id` kerak;
- compacted-topiclarda va tranzaksiyalarda offsetlar oraliq tashlab ketishi mumkin;
- consumer **committed offset** saqlaydi: bu o'qilishi kerak bo'lgan **keyingi** yozuvning raqami.

Partitiondagi asosiy pozitsiyalar:

```text
 offset:  0   1   2   3   4   5   6   7
         [ ] [ ] [ ] [ ] [ ] [ ] [ ] [ ]
          ^           ^           ^       ^
          |           |           |       |
   log start    committed    high         log end
   offset       offset       watermark    offset (LEO)
                (guruhniki)  (HW)
```

- **Log Start Offset**: mavjud bo'lgan eng eski yozuv (eskilari retention bo'yicha o'chirilgan).
- **Committed Offset**: muayyan consumer group qayergacha o'qigani.
- **High Watermark (HW)**: ISRdagi barcha replikalar tasdiqlagan oxirgi yozuv. Consumer yozuvlarni faqat HWgacha ko'radi.
- **Log End Offset (LEO)**: leaderda keyingi yozuv yoziladigan pozitsiya.
- **Consumer lag** = HW − committed offset.

## 1.7 Broker

**Broker** quyidagilarni bajaradigan Kafka jarayoni (JVM):

- producerdan yozuvlarni qabul qiladi va ularni diskka yozadi;
- yozuvlarni consumerga beradi;
- boshqa brokerlardagi partitionlarni replikatsiya qiladi;
- iste'molchilar guruhlariga xizmat ko'rsatadi (group coordinator);
- guruhlarning offsetlarini `__consumer_offsets` ichki topicida saqlaydi.

Klient bitta brokerning manzilini (`bootstrap.servers`) bilishi yetarli. U butun klasterning metadatasini oladi va bundan keyin kerakli partitionlarning leaderlariga to'g'ridan-to'g'ri murojaat qiladi.

```text
1. client --> bootstrap broker: Metadata request
2. broker --> client: brokerlar ro'yxati, topiclar, partition leaderlari
3. client --> leader partition-0 (broker-2): Produce / Fetch
```

> Kafkani Dockerda ishga tushirishdagi eng ko'p uchraydigan xato: noto'g'ri `advertised.listeners`. Broker klientga shunday manzil beradiki, klient keyin u orqali ulana olmaydi. Buni 2-modulda ko'rib chiqamiz.

## 1.8 KRaft: ZooKeepersiz Kafka

3.x versiyasigacha Kafka metadatani (topiclar ro'yxati, leaderlar, konfiguratsiyalar, ACL) **Apache ZooKeeper** da saqlar edi. **Kafka 4.0 da ZooKeeper butunlay olib tashlangan**, endi yagona ish rejimi **KRaft** (Kafka Raft).

```text
            KRaft controller quorum
     +-------------+-------------+-------------+
     | controller-1| controller-2| controller-3|
     |   (leader)  |  (follower) |  (follower) |
     +-------------+-------------+-------------+
              | metadata jurnali __cluster_metadata
              v
     +---------+   +---------+   +---------+
     |broker-1 |   |broker-2 |   |broker-3 |
     +---------+   +---------+   +---------+
```

KRaft qanday ishlaydi:

- metadata `__cluster_metadata` ichki topicida **hodisalar jurnali** sifatida saqlanadi;
- controllerlar leaderni **Raft** protokoli bo'yicha saylaydi;
- faol controller qarorlar qabul qiladi: topiclarni yaratish, partition leaderlarini tanlash, broker ishdan chiqqanda chora ko'rish;
- brokerlar metadata o'zgarishlarini inkremental tarzda oladi va ularni xotirada saqlaydi.

KRaft nima beradi:

- ikkita tizim o'rniga bitta (ZooKeeperni alohida ekspluatatsiya qilish shart emas);
- controllerning tez failoveri (daqiqalar o'rniga soniyalar);
- klasterda **millionlab partitionlarni** qo'llab-quvvatlash;
- brokerlarning tez ishga tushishi va shutdowni.

**Tugunlar rollari** (`process.roles`):

| Qiymat | Qachon ishlatiladi |
|---|---|
| `broker,controller` (combined) | Dasturlash, testlar, kichik klasterlar |
| `controller` | Production: 3 yoki 5 ta ajratilgan controller |
| `broker` | Production: ma'lumot saqlaydigan brokerlar |

3 ta controllerdan iborat kvorum 1 ta tugunning ishdan chiqishiga, 5 ta controllerdan iborat kvorum esa 2 ta tugunning ishdan chiqishiga bardosh beradi. Controllerlar sonining juft bo'lishi bardoshlilikni oshirmaydi.

> **ZooKeeperdan migratsiya:** ZooKeeperdagi klasterdan birdaniga 4.x ga yangilanib bo'lmaydi. Avval 3.9 ga o'tib, metadatani KRaftga migratsiya qilish kerak, shundan keyingina 4.x ga yangilanadi.

## 1.9 Birinchi mental model

```text
Producer key bilan record yozadi
   -> key xeshlanadi -> partition tanlanadi
   -> yozuv partition leaderiga ketadi
   -> leader uni diskdagi log oxiriga qo'shib yozadi
   -> followerlar yozuvni nusxalaydi
   -> yozuv consumer uchun ochiladi (high watermark gacha)
   -> consumer batchlab o'qiydi va offsetni commit qiladi
   -> yozuv retention muddati tugaguncha diskda turadi
```

### O'z-o'zini tekshirish uchun savollar

1. Nima uchun offsetni xabarning global IDsi sifatida ishlatib bo'lmaydi?
2. High watermark nima va nima uchun consumer undan keyingi yozuvlarni ko'rmaydi?
3. Kafka nima uchun ZooKeeperdan voz kechdi?
4. Nima uchun controllerlar kvorumi uchun 4 ta emas, 3 yoki 5 ta tugun olinadi?

---

# Modul 2. Kafkani Dockerda o'rnatish va birinchi buyruqlar

## 2.1 Kafkani eng tez ishga tushirish (bitta tugun)

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.3.1
```

`apache/kafka` image standart holatda KRaft rejimida, broker va controller rollari bilan bitta tugunni ishga tushiradi, u `localhost:9092` ni tinglaydi.

Tekshiramiz:

```bash
docker ps
docker logs kafka | grep -i "started"
```

## 2.2 Birinchi topic, birinchi xabar

Topic yaratamiz:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic hello-kafka --partitions 3 --replication-factor 1
```

Tavsifini ko'ramiz:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic hello-kafka
```

Xabarlar yozamiz (1-terminal):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka
```

```text
> hello
> my first kafka event
> order-123 created
```

O'qiymiz (2-terminal):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --from-beginning
```

Uchala xabarni ham ko'rasiz, **lekin tartib** yuborilgan tartibdan **farq qilishi mumkin**. Nima uchun? Topicda 3 ta partition bor, kalitsiz yozuvlar esa ular bo'yicha taqsimlanadi. Tartib faqat partition ichida kafolatlangan.

## 2.3 Kalitli xabarlar

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --property parse.key=true \
  --property key.separator=:
```

```text
> user-1:login
> user-2:login
> user-1:add_to_cart
> user-1:checkout
```

Kalit, partition va offsetni chiqarib o'qiymiz:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

`user-1` ning barcha hodisalari bitta partitionga tushadi va qat'iy tartibda keladi.

## 2.4 Docker Composeda uchta brokerdan iborat klaster

Replikatsiyani o'rganish uchun haqiqiy klaster kerak. Direktoriya va `docker-compose.yml` faylini yarating:

```bash
mkdir kafka-course && cd kafka-course
```

```yaml
x-kafka-common: &kafka-common
  image: apache/kafka:4.3.1
  restart: unless-stopped

x-kafka-env: &kafka-env
  CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
  KAFKA_PROCESS_ROLES: "broker,controller"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_MIN_INSYNC_REPLICAS: 2
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
  KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

services:
  kafka-1:
    <<: *kafka-common
    container_name: kafka-1
    ports: ["19092:19092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:19092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-1:9092,EXTERNAL://localhost:19092"

  kafka-2:
    <<: *kafka-common
    container_name: kafka-2
    ports: ["29092:29092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:29092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-2:9092,EXTERNAL://localhost:29092"

  kafka-3:
    <<: *kafka-common
    container_name: kafka-3
    ports: ["39092:39092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:39092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-3:9092,EXTERNAL://localhost:39092"

  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    container_name: kafka-ui
    ports: ["8080:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: "course"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    depends_on: [kafka-1, kafka-2, kafka-3]
```

Ishga tushirish:

```bash
docker compose up -d
docker compose ps
```

Kafka UI web-interfeysi `http://localhost:8080` manzilida ochiladi.

## 2.5 Listeners va advertised.listeners: klient nima uchun ulanmayapti

«Kafka Dockerda ishlamayapti» degan savollarning eng ko'p uchraydigan sababi shu.

```text
KAFKA_LISTENERS            broker qaysi interfeys va portlarda TINGLAYDI
KAFKA_ADVERTISED_LISTENERS broker klientlarga qaysi manzilni MA'LUM QILADI
```

Klient avval `bootstrap.servers` ga ulanadi, metadatani oladi va shundan keyin **advertised-manzillarga** murojaat qiladi. Shuning uchun:

| Klient qayerdan | Qaysi listenerdan foydalanadi | Manzil |
|---|---|---|
| Xuddi shu Docker tarmog'idagi boshqa konteyner | `INTERNAL` | `kafka-1:9092` |
| Noutbukingizdagi ilova | `EXTERNAL` | `localhost:19092` |
| Controllerlar o'zaro | `CONTROLLER` | `kafka-1:9093` |

Agar advertised-manzil `kafka-1:9092` bo'lsa, hostdagi ilova uni metadatada oladi va `UnknownHostException` xatosi bilan yoki cheksiz qayta ulanish urinishlari bilan yiqiladi.

## 2.6 KRaft-kvorumni tekshiramiz

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --status
```

`LeaderId`, `CurrentVoters`, `HighWatermark` ni va har bir controllerning ortda qolishini ko'rasiz:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --replication
```

## 2.7 Kafka ma'lumotlarni diskda qayerda saqlaydi

```bash
docker exec kafka-1 ls /tmp/kraft-combined-logs
```

```text
orders-0/
├── 00000000000000000000.log        # yozuvlarning o'zi
├── 00000000000000000000.index      # offset -> fayldagi pozitsiya
├── 00000000000000000000.timeindex  # timestamp -> offset
├── leader-epoch-checkpoint
└── partition.metadata
```

> `apache/kafka` imageda ma'lumotlar standart holatda konteyner ichidagi `/tmp/kraft-combined-logs` da turadi. Uzoq muddatli saqlash uchun volume ulang va `KAFKA_LOG_DIRS` ni belgilang.

## 2.8 Lokal dasturlash uchun muqobillar

- **Testcontainers** (`org.testcontainers:kafka`, Go va Python uchun modul): integratsion testlardagi Kafka.
- **Redpanda**: C++ da yozilgan, Kafka bilan mos keluvchi broker, lokal testlar uchun qulay, lekin bu o'ziga xos xususiyatlari bor boshqa mahsulot.
- **Managed Kafka**: Confluent Cloud, Amazon MSK, Aiven, Yandex Managed Service for Apache Kafka, VK Cloud va boshqalar. O'z ekspluatatsiya jamoasi bo'lmagan production uchun ko'pincha bu eng yaxshi tanlov.

### Amaliyot

1. Uchta brokerdan iborat klasterni ko'taring.
2. 3 ta partitionli `hello-kafka` topicini yarating va kalitli 10 ta xabar yuboring.
3. Kafka UIda har bir kalit qaysi partitionga tushganini toping.
4. KRaft-kvorum holatini tekshiring va qaysi tugun leader ekanini aniqlang.

---

# Modul 3. Partitionlar, kalitlar va xabarlar tartibi

## 3.1 Producer partitionni qanday tanlaydi

```text
             key != null                         key == null
                 |                                    |
   partition = murmur2(key) % numPartitions   sticky partitioner:
                 |                            batchni bitta partitionga yozadi,
                 v                            keyin boshqasiga o'tadi
    bir xil key -> bir xil partition          (bir tekis va yaxshi batching bilan)
```

Qoidalar:

1. Agar yozuvda **partition aniq ko'rsatilgan** bo'lsa, o'sha ishlatiladi.
2. Agar **key** bo'lsa, partition = `murmur2(key) mod partitionlar_soni`.
3. Agar key bo'lmasa, **built-in sticky partitioner** ishlaydi: u bitta partition uchun batchni to'ldiradi va keyin boshqasiga o'tadi. Bu yirik batchlar va past kechikish beradi.
4. **O'z Partitioneringizni** yozish mumkin (masalan, «qaynoq» kalitlar uchun).

## 3.2 Nima uchun global tartib yo'q

```text
Yuborildi: A1, B1, A2, B2, A3

partition-0 (key A): A1 -> A2 -> A3   tartib saqlangan
partition-1 (key B): B1 -> B2         tartib saqlangan

Consumer quyidagicha olishi mumkin: A1, B1, B2, A2, A3
```

Butun topic bo'yicha global tartib faqat **bitta partition** bilan mumkin, bu esa masshtablashni yo'qqa chiqaradi. To'g'ri savol shunday: **biznesga qanday tartib kerak?** Odatda tartib **bitta obyekt doirasida** kerak: bitta buyurtma, bitta hisob, bitta foydalanuvchi.

## 3.3 Partitsiyalash kalitini qanday tanlash kerak

| Vazifa | Yaxshi kalit | Yomon kalit |
|---|---|---|
| Buyurtmaning hayot sikli | `order_id` | `event_type` (barcha `OrderCreated` bitta partitionda) |
| Bank hisobi balansi | `account_id` | `transaction_id` (yechishlar va to'ldirishlar aralashib ketadi) |
| Foydalanuvchi harakatlari | `user_id` | `country` (Rossiya bitta partitionni egallaydi) |
| Datchiklar metrikalari | `device_id` | `timestamp` |
| Tartibga talab qo'yilmagan loglar | `null` | 3 ta host va 50 ta partition bo'lganda `hostname` |

**Bank hisobi haqidagi masala.** `acc-1` hisobi bo'yicha hodisalar:

```text
Deposit +1000
Withdraw -700
Withdraw -500
```

Agar kalit `transaction_id` bo'lsa, hodisalar turli partitionlarga tushadi va consumer `-500` ni `+1000` dan oldin qayta ishlashi mumkin: balans yetarli bo'la turib operatsiya rad etiladi. `account_id` kaliti ketma-ket qayta ishlashni kafolatlaydi.

## 3.4 Qaynoq kalitlar (hot partitions)

Agar bitta kalit trafikning 40% ini hosil qilsa (yirik sotuvchi, mashhur strimer), uning partitioni ortiqcha yuklanadi, shu partitionning consumeri esa ortda qoladi.

Yechimlar:

- **tarkibiy kalit** `seller_id + bucket`, bunda bucket = `hash(order_id) % 8`, agar tartib faqat buyurtma doirasida kerak bo'lsa;
- qaynoq klientni **alohida topicga** chiqarish;
- ma'lum qaynoq kalitlar ro'yxati uchun **maxsus (custom) partitioner**;
- tartibga qo'yilgan talablarni qayta ko'rib chiqish.

## 3.5 Tuzoq: partitionlar sonini oshirish

Partitionlar sonini faqat oshirish mumkin, kamaytirib bo'lmaydi.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --alter --topic orders --partitions 12
```

Lekin `hash(key) % N` formulasi o'zgaradi:

```text
6 ta partition edi:    hash("order-123") % 6  = 1
12 ta partition bo'ldi: hash("order-123") % 12 = 7
```

Buyurtmaning yangi hodisalari partition 7 ga ketadi, eskilari esa partition 1 da qolgan. Consumer yangi hodisalarni eskilaridan oldin qayta ishlashi mumkin. **O'tish davrida kalit bo'yicha tartib buziladi.**

Bu bilan qanday yashash kerak:

- partitionlar sonini 1-2 yillik o'sishni hisobga olib, **zaxira bilan** belgilang;
- partitionlarni eski ma'lumotlar qayta ishlanib bo'lgan va obyektlar «yopilgan» paytda oshiring;
- qat'iy talablar uchun **yangi topic** yarating va iste'molchilarni unga ko'chiring.

## 3.6 Topicga nechta partition kerak

Baholash uchun formula:

```text
partitions >= max( T / Tp , T / Tc )

T  - topicning maqsadli o'tkazuvchanligi (MB/s)
Tp - bitta partition yozishda necha MB/s ga bardosh beradi
Tc - bitta consumer necha MB/s ni qayta ishlaydi
```

Misol: 100 MB/s kerak, bitta consumer 10 MB/s ni qayta ishlaydi, bitta partition 50 MB/s ni qabul qiladi.

```text
max(100/50, 100/10) = max(2, 10) = 10 partitions
+ o'sish uchun zaxira x2 = 20 partitions
```

Amaliy mo'ljallar:

- kichik topic: 3-6 ta partition;
- o'rtacha servis: 12-24 ta partition;
- yuqori yuklamali oqim: 50-200+ ta partition;
- bo'luvchilari ko'p sonlarni tanlash qulay: 6, 12, 24, 48, 60.

Partitionlarning haddan tashqari ko'pligi ham yomon: brokerlarda fayllar va xotira ko'proq sarflanadi, rebalance uzoqroq davom etadi, metadata ko'payadi, `acks=all` da end-to-end kechikish ortadi.

### Amaliyot

1. 6 ta partitionli `accounts` topicini yarating.
2. `acc-1`, `acc-2`, `acc-3` kalitlari uchun 5 tadan hodisa yuboring va bitta hisobning hodisalari bitta partitionda ekanini tekshiring.
3. Partitionlar sonini 12 taga oshiring, yana hodisalar yuboring va «ko'chib o'tgan» kalitni toping.

---

# Modul 4. Kafka replikatsiyasi: leader, ISR, acks, min.insync.replicas

## 4.1 Replication factor

Har bir partitionning N ta nusxasi bor (**replication factor**). Bitta nusxa **leader**, qolganlari **followers**.

```text
topic orders, partition-0, replication.factor=3

broker-1: partition-0 (LEADER)   <-- producer yozadi, consumer o'qiydi
broker-2: partition-0 (follower) <-- leaderdan nusxalaydi
broker-3: partition-0 (follower) <-- leaderdan nusxalaydi
```

- Producer har doim **leaderga** yozadi.
- Consumer standart holatda **leaderdan** o'qiydi (KIP-392 bilan `client.rack` va `replica.selector.class` orqali eng yaqin replikadan o'qish mumkin).
- Followerlar oddiy consumer kabi leaderga doimiy ravishda fetch-so'rovlar yuboradi.

## 4.2 ISR (In-Sync Replicas)

**ISR** leaderga ulgurib borayotgan replikalar to'plami. Replika leaderga `replica.lag.time.max.ms` dan (standart qiymati 30 soniya) uzoqroq yetib ololmasa, ISRdan chiqib qoladi.

```text
Replicas: 1,2,3   partitionga tayinlangan barcha replikalar
ISR:      1,3     ayni paytda leader bilan sinxron replikalar
```

ISRdan tashqaridagi replika oddiy saylovda leader bo'la olmaydi. Aks holda tasdiqlangan ma'lumotlarni yo'qotamiz.

## 4.3 acks: yozuvni tasdiqlash

| `acks` | Producer qachon OK oladi | Yo'qotish xavfi | Kechikish |
|---|---|---|---|
| `0` | Soketga yuborilgan zahoti | Yuqori: broker yozuvni olmagan bo'lishi mumkin | Minimal |
| `1` | Leader o'z logiga yozganda | O'rtacha: leader replikatsiyagacha o'lgan bo'lishi mumkin | Past |
| `all` (`-1`) | ISRdagi barcha replikalar yozganda | `min.insync.replicas` to'g'ri bo'lsa minimal | Yuqoriroq |

Kafka 3.0 dan boshlab producerning standart qiymatlari: `acks=all` va `enable.idempotence=true`.

## 4.4 Nima uchun min.insync.replicas bo'lmasa acks=all yetarli emas

`acks=all` «**joriy ISRdagi** barcha replikalar» degan ma'noni bildiradi. Agar ISR bitta leadergacha qisqargan bo'lsa, `acks=all` amalda `acks=1` ga aylanadi.

```text
replication.factor=3, min.insync.replicas=1 (standart qiymat)

ISR = {1}            <- ikkita follower ortda qoldi
producer acks=all    <- yozuvni faqat leader tasdiqladi
broker-1 o'ladi      <- tasdiqlangan ma'lumotlar YO'QOLDI
```

**`min.insync.replicas`** broker `acks=all` bilan kelgan yozuvlarni qabul qiladigan ISRning minimal o'lchamini belgilaydi. Agar ISR undan kichik bo'lsa, producer `NotEnoughReplicasException` xatosini oladi va ma'lumotlar jimgina yo'qolmaydi.

**Ishonchlilikning oltin standarti:**

```properties
# topic / broker
replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false

# producer
acks=all
enable.idempotence=true
```

Bu konfiguratsiya bitta broker ishdan chiqqanda ma'lumot yo'qotmaydi va yozishni ham to'xtatmaydi.

| RF | min.insync.replicas | Yozuvni yo'qotmasdan nechta broker ishdan chiqishiga bardosh beradi | Nechta broker ishdan chiqqanda yozish davom etadi |
|---|---|---|---|
| 3 | 1 | Kafolat yo'q | 2 ta broker |
| 3 | 2 | 1 ta broker | 1 ta broker |
| 3 | 3 | 2 ta broker | 0 ta broker (yozish to'xtaydi) |
| 5 | 3 | 2 ta broker | 2 ta broker |

## 4.5 High watermark va leader epoch

- Yozuv ISRdagi barcha replikalarga replikatsiya qilingach, leader **high watermark** ni oldinga suradi.
- Consumerlar faqat HWgacha bo'lgan yozuvlarni ko'radi. Shuning uchun ular leader almashganda yo'qolib qolishi mumkin bo'lgan yozuvni o'qimaydi.
- **Leader epoch** leaderlik «davri»ning raqami. Leader almashganda followerlar epoch bo'yicha replikalar orasida tafovut bo'lmasligi uchun logning qaysi oxirgi qismini kesib tashlash kerakligini tushunadi.

## 4.6 Unclean leader election

Agar ISRdagi barcha replikalar o'lgan va faqat ortda qolgan replika qolgan bo'lsa, tanlov bor:

| `unclean.leader.election.enable` | Xatti-harakat | Narxi |
|---|---|---|
| `false` (standart qiymat) | ISRdagi replika qaytmaguncha partition ishlamaydi | Mavjudlikni (availability) yo'qotish |
| `true` | Ortda qolgan replika leader bo'ladi | Tasdiqlangan ma'lumotlarni yo'qotish |

To'lovlar va buyurtmalar uchun konsistentlik (`false`) tanlanadi. Metrikalar va loglar uchun ba'zan `true` ga yo'l qo'yish mumkin.

## 4.7 Amaliyot: production-like topic yaratamiz

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create --topic orders \
  --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2
```

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic orders
```

```text
Topic: orders  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=2
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  ...
```

Leaderlar brokerlar bo'yicha bir tekis taqsimlangan. `Replicas` ro'yxatidagi birinchi replika **preferred leader** hisoblanadi.

## 4.8 Amaliyot: brokerni o'ldiramiz

```bash
docker stop kafka-1

docker exec kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:9092 --describe --topic orders
```

```text
Edi:    Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Bo'ldi: Partition: 0  Leader: 2  Replicas: 1,2,3  Isr: 2,3
```

Controller ISRdan yangi leaderni tanladi. Producer va consumer metadatani yangilab, ishni davom ettirdi.

Faqat muammoli partitionlarni ko'rsatamiz:

```bash
docker exec kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:9092 --describe --under-replicated-partitions
```

Endi **ikkinchi brokerni to'xtating** va yozib ko'ring:

```bash
docker stop kafka-2
docker exec -it kafka-3 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-3:9092 --topic orders \
  --producer-property acks=all
```

`NotEnoughReplicas` olasiz: ISR = 1 < `min.insync.replicas` = 2. Kafka ishonchli saqlay olmaydigan yozuvni qabul qilishdan **bosh tortadi**. Bu to'g'ri xatti-harakat.

Brokerlarni qaytaramiz:

```bash
docker start kafka-1 kafka-2
```

Brokerlar replikatsiyada yetib olgach, ISRga qaytadi. Kafka leaderlikni vaqti-vaqti bilan preferred-replikalarga qaytaradi (`auto.leader.rebalance.enable=true`). Buni qo'lda bajarish:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server kafka-1:9092 --election-type preferred --all-topic-partitions
```

## 4.9 Rack awareness

Agar brokerlar turli stoykalarda (rack) yoki turli availability zonalarda joylashgan bo'lsa, `broker.rack` ni ko'rsating. Kafka bitta partitionning replikalarini **turli zonalarga** joylashtiradi va butun bir zonaning ishdan chiqishi barcha nusxalarni yo'q qilmaydi.

```properties
broker.rack=eu-central-1a
```

## 4.10 Antipattern: replication.factor = 1

```text
replication.factor=1
```

Bitta broker o'ldi, disk buzildi, va partition ishlamaydi yoki butunlay yo'qoladi. Productiondagi har qanday muhim ma'lumot uchun `replication.factor=3` dan foydalaning.

### O'z-o'zini tekshirish uchun savollar

1. Replica ISRdan nimasi bilan farq qiladi?
2. Nima uchun `min.insync.replicas=2` bo'lmasa `acks=all` ma'lumotlarning saqlanishini kafolatlamaydi?
3. RF=3, min.insync.replicas=2 va ikkita broker ishdan chiqqan holatda yozish bilan nima sodir bo'ladi?
4. Unclean leader election nima va u qachon yoqiladi?
5. `broker.rack` nima uchun kerak?

---

# Modul 5. Kafka Producer: qanday tuzilgan va qanday sozlanadi

## 5.1 Producerning ichki tuzilishi

```text
send(record)
   |
   v
[Serializer] key/value -> bytes
   |
   v
[Partitioner] partitionni tanlaydi
   |
   v
[RecordAccumulator]  partitionlar bo'yicha batch buferlari (buffer.memory)
   |   batch orders-0: [r1][r2][r3]
   |   batch orders-1: [r4]
   v
[Sender thread] tayyor batchlarni oladi (batch.size yoki linger.ms)
   |   leader-broker bo'yicha guruhlaydi, siqadi
   v
Network -> Broker leader -> javob -> callback / Future
```

Asosiy jihat: `send()` **asinxron**. U yozuvni buferga qo'yadi va darhol `Future` qaytaradi. Haqiqiy yuborish Sender fon oqimida (thread) sodir bo'ladi.

## 5.2 Batching: linger.ms va batch.size

| Parametr | Standart qiymat | Ma'nosi |
|---|---|---|
| `batch.size` | 16384 (16 KB) | Bitta partition uchun batchning maksimal o'lchami |
| `linger.ms` | 5 (Kafka 4.0 dan) | Yuborishdan oldin batch to'lishini qancha kutish |
| `buffer.memory` | 33554432 (32 MB) | Producerning umumiy buferi |
| `max.block.ms` | 60000 | Bufer to'la yoki metadata yo'q bo'lsa, `send()` qancha vaqt bloklanadi |
| `compression.type` | `none` | `gzip`, `snappy`, `lz4`, `zstd` |

Batch **`batch.size` to'lganda** yoki **`linger.ms` tugaganda** yuboriladi, qaysi biri oldin yuz bersa.

```text
linger.ms=0   ko'plab kichik so'rovlar, past kechikish, past throughput
linger.ms=20  yirik batchlar, yaxshiroq siqish, throughput bir necha baravar yuqori
```

## 5.3 Siqish

| Algoritm | Siqish darajasi | CPU | Qachon ishlatiladi |
|---|---|---|---|
| `lz4` | O'rtacha | Past | Yuqori yuklama uchun yaxshi standart tanlov |
| `zstd` | Yuqori | O'rtacha | Trafik va diskni tejash, JSON-loglar |
| `snappy` | O'rtacha | Past | Eski tizimlar bilan moslik |
| `gzip` | Yuqori | Yuqori | Kamdan-kam, har bir bayt muhim bo'lganda |

Siqish batch darajasida ishlaydi: batch qancha katta bo'lsa, siqish shuncha yaxshi. Broker ma'lumotlarni qayta siqmasligi uchun topicda `compression.type=producer` ni saqlang.

## 5.4 Retries va timeoutlar

```text
|<------------------- delivery.timeout.ms (120 s) ------------------->|
| linger | request.timeout.ms | retry.backoff | request.timeout.ms | ...
```

| Parametr | Standart qiymat | Ma'nosi |
|---|---|---|
| `retries` | `2147483647` | Amalda cheksiz, uni `delivery.timeout.ms` cheklaydi |
| `delivery.timeout.ms` | 120000 | Barcha qayta urinishlarni qo'shib hisoblaganda yozuvni yetkazishga ajratilgan umumiy vaqt |
| `request.timeout.ms` | 30000 | Bitta so'rovga javobni kutish |
| `retry.backoff.ms` | 100 | Qayta urinishlar orasidagi pauza (`retry.backoff.max.ms` gacha eksponensial o'sadi) |

Retrylar faqat idempotentlik bilan xavfsiz. Aks holda brokerning javobi yo'qolsa, yozuv takrorlanib qoladi.

## 5.5 Idempotent producer

Idempotentliksiz muammo:

```text
producer --> broker: batch #1
broker batch #1 ni logga yozadi
broker --> producer: ACK   (javob tarmoqda yo'qoldi)
producer: timeout, retry batch #1
broker batch #1 ni YANA BIR MARTA yozadi   <-- dublikat
```

`enable.idempotence=true` bilan:

- producer **Producer ID (PID)** oladi;
- har bir batch partition doirasida **sequence number** oladi;
- broker avval ko'rilgan sequence numberli batchni tashlab yuboradi.

Talablar: `acks=all`, `max.in.flight.requests.per.connection <= 5`. Partition ichidagi tartib retrylarda ham saqlanadi.

> Idempotentlik **bitta producer nusxasining retrylari tufayli** paydo bo'ladigan dublikatlardan himoya qiladi. Agar ilova yiqilib, o'sha biznes-hodisani qaytadan yuborsa, bu yangi PID va dublikat. Buning uchun `event_id` va idempotent consumer kerak.

## 5.6 Javada producer

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>4.3.1</version>
</dependency>
```

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ishonchlilik
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // unumdorlik
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "order-service");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String orderId = "order-" + i;
                String value = "{\"order_id\":\"" + orderId + "\",\"amount\":" + (i * 100) + "}";
                ProducerRecord<String, String> record = new ProducerRecord<>("orders", orderId, value);
                record.headers().add("event_type", "OrderCreated".getBytes());

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        // delivery.timeout.ms tugaganidan keyin shu yerga tushamiz
                        System.err.println("Yuborib bo'lmadi " + orderId + ": " + exception);
                    } else {
                        System.out.printf("%s -> partition=%d offset=%d%n",
                                orderId, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
    }
}
```

**Qoidalar:**

- **har bir ilovaga bitta `KafkaProducer`** yarating: u thread-safe va uni yaratish qimmat;
- **callbackdagi xatoni har doim qayta ishlang**, aks holda ma'lumot yo'qolishi sezilmay qoladi;
- ilova to'xtayotganda `flush()` va `close()` ni chaqiring, aks holda bufer yo'qoladi;
- qaynoq yo'lda (hot path) har bir yozuv uchun `send().get()` qilmang: bu batchingni yo'qqa chiqaradi.

## 5.7 Goda producer (franz-go)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:19092", "localhost:29092", "localhost:39092"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		rec := &kgo.Record{
			Topic: "orders",
			Key:   []byte(fmt.Sprintf("order-%d", i)),
			Value: []byte(fmt.Sprintf(`{"order_id":"order-%d"}`, i)),
		}
		cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				fmt.Println("error:", err)
				return
			}
			fmt.Printf("partition=%d offset=%d\n", r.Partition, r.Offset)
		})
	}
	if err := cl.Flush(ctx); err != nil {
		panic(err)
	}
}
```

Go uchun mashhur Kafka klientlari: `franz-go` (sof Go, protokolni to'liq qo'llab-quvvatlaydi), `confluent-kafka-go` (librdkafka ustidagi wrapper), `segmentio/kafka-go`.

## 5.8 Pythonda producer (confluent-kafka)

```bash
pip install confluent-kafka
```

```python
import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 20,
    "compression.type": "lz4",
})

def on_delivery(err, msg):
    if err:
        print(f"Yetkazishda xato: {err}")
    else:
        print(f"{msg.key().decode()} -> partition={msg.partition()} offset={msg.offset()}")

for i in range(1, 11):
    order = {"order_id": f"order-{i}", "amount": i * 100}
    producer.produce("orders", key=order["order_id"], value=json.dumps(order), callback=on_delivery)
    producer.poll(0)  # callbacklarni qayta ishlash

producer.flush()
```

## 5.9 Producer sozlamalari profillari

| Maqsad | Sozlamalar |
|---|---|
| **Maksimal ishonchlilik** (to'lovlar) | `acks=all`, `enable.idempotence=true`, topic RF=3 va `min.insync.replicas=2`, yoki tranzaksiyalar |
| **Maksimal throughput** (loglar, kliklar) | `linger.ms=50-100`, `batch.size=256KB-1MB`, `compression.type=zstd` yoki `lz4`, kattaroq `buffer.memory` |
| **Minimal kechikish** | `linger.ms=0`, kichik batchlar, `compression.type=none` yoki `lz4` |

### Amaliyot

1. Java producerni ishga tushiring va kalitlar partitionlar bo'yicha qanday taqsimlanganini ko'ring.
2. `linger.ms=0` va `linger.ms=50` bilan o'tkazuvchanlikni o'lchang:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic orders --num-records 1000000 --record-size 512 --throughput -1 \
  --producer-props bootstrap.servers=kafka-1:9092 acks=all linger.ms=0 compression.type=none

docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic orders --num-records 1000000 --record-size 512 --throughput -1 \
  --producer-props bootstrap.servers=kafka-1:9092 acks=all linger.ms=50 batch.size=262144 compression.type=lz4
```

3. `records/sec`, `avg latency` va `99th` persentilni solishtiring.

---

# Modul 6. Kafka Consumer va Consumer Groups

## 6.1 Poll loop: consumer qanday o'qiydi

Kafkada consumer **pull** modeli bo'yicha ishlaydi: u ma'lumotlarni brokerdan o'zi so'raydi.

```text
while (running) {
    records = consumer.poll(timeout)     // batchlab fetch, heartbeat, rebalance
    for record in records:
        process(record)
    consumer.commit()                     // progressni saqlash
}
```

`poll()` ichida nima sodir bo'ladi:

- tayinlangan partitionlarning leaderlariga fetch-so'rovlar yuboriladi;
- guruh protokolida ishtirok etiladi (heartbeat, rebalance);
- to'plangan yozuvlar qaytariladi (`max.poll.records` gacha);
- `enable.auto.commit` yoqilgan bo'lsa, avtomatik commit qilinadi.

## 6.2 Consumer Group

**Consumer group** bitta `group.id` ga ega bo'lgan va **partitionlarni o'zaro bo'lib oladigan** consumerlar to'plami.

```text
topic orders: 6 partitions

group "billing" (3 consumers)          group "analytics" (1 consumer)
  consumer-1: p0, p1                     consumer-1: p0..p5
  consumer-2: p2, p3
  consumer-3: p4, p5
```

Qoidalar:

- **bitta partitionni guruh ichida aynan bitta consumer o'qiydi**;
- turli guruhlar topicni **mustaqil** o'qiydi va o'z offsetlarini saqlaydi;
- o'qishni masshtablash = guruhga consumer qo'shish.

## 6.3 Consumerlar partitionlardan ko'p bo'lsa

```text
6 partitions, 8 consumers

consumer-1..6: bittadan partition
consumer-7:    bo'sh turadi
consumer-8:    bo'sh turadi
```

**20 ta consumer 10 ta partitionli topicni tezlashtirmaydi.** Ortiqcha consumerlar qaynoq zaxira (hot standby) sifatida ishlaydi. Ko'proq parallellik kerak bo'lsa, partitionlarni ko'paytiring yoki yozuvlarni consumer ichida parallel qayta ishlang (kalit bo'yicha tartibni saqlagan holda).

## 6.4 Offsetlar va commit

Guruhning progressi `__consumer_offsets` ichki topicida (compacted) saqlanadi. Commit «guruh offset N gacha hammasini qayta ishladi, keyingi o'qiladigani N» degan ma'noni bildiradi.

| Rejim | Qanday ishlaydi | Xavf |
|---|---|---|
| `enable.auto.commit=true` (standart qiymat) | `poll()` ichida har `auto.commit.interval.ms` da (5 s) commit | Yiqilganda dublikatlar; qayta ishlash asinxron bo'lsa, yo'qotish |
| `commitSync()` | Batch qayta ishlangandan keyin bloklovchi commit | Throughput pastroq, lekin oldindan aytib bo'ladigan natija |
| `commitAsync()` | Bloklamaydigan commit | Xato bo'lsa qayta urinish yo'q; oxirida `commitSync()` kerak |
| O'z ma'lumotlar bazangizda commit | Offset natija bilan bitta tranzaksiyada saqlanadi | Murakkabroq, lekin exactly-once effektini beradi |

**`auto.offset.reset`** guruhda saqlangan offset bo'lmasa, qayerdan o'qishni belgilaydi:

- `latest` (standart qiymat): faqat yangi xabarlar;
- `earliest`: eng boshidan;
- `none`: xato tashlash.

## 6.5 Yetkazish semantikalari

```text
AT-MOST-ONCE (ko'pi bilan bir marta)
  poll -> commit -> process
  process paytida yiqildik -> xabar YO'QOLDI

AT-LEAST-ONCE (kamida bir marta)
  poll -> process -> commit
  commitdan oldin yiqildik -> xabar QAYTA ishlanadi

EXACTLY-ONCE (aynan bir marta)
  Kafka tranzaksiyalari (Kafka ichida read-process-write)
  yoki at-least-once + idempotent qayta ishlash
```

**Production qoidasi:** at-least-once dan foydalaning va qayta ishlashni **idempotent** qiling. Taqsimlangan tizimlarda dublikatlar muqarrar: retrylar, rebalance, yiqilishlar.

## 6.6 Javada qo'lda commit qiladigan consumer

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class BillingConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        // guruhlarning yangi protokoli (KIP-848), 6.9 bo'limga qarang
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Ctrl+C bosilganda to'g'ri yakunlash
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try { mainThread.join(); } catch (InterruptedException ignored) {}
        }));

        try {
            consumer.subscribe(List.of("orders"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    process(r); // idempotent bo'lishi kerak
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // odatiy to'xtash
        } finally {
            consumer.close(); // guruhni session timeoutni kutmasdan tez tark etish
        }
    }

    static void process(ConsumerRecord<String, String> r) {
        System.out.printf("key=%s partition=%d offset=%d value=%s%n",
                r.key(), r.partition(), r.offset(), r.value());
    }
}
```

**Muhim:** `KafkaConsumer` **thread-safe emas**. Bitta consumer = bitta thread. Boshqa threaddan chaqirish xavfsiz bo'lgan yagona metod `wakeup()`.

## 6.7 Python va Goda consumer

**Python (confluent-kafka):**

```python
from confluent_kafka import Consumer, KafkaException

consumer = Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "billing",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(msg.key(), msg.partition(), msg.offset(), msg.value())
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()
```

**Go (franz-go):**

```go
cl, _ := kgo.NewClient(
	kgo.SeedBrokers("localhost:19092"),
	kgo.ConsumerGroup("billing"),
	kgo.ConsumeTopics("orders"),
	kgo.DisableAutoCommit(),
	kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
)
defer cl.Close()

for {
	fetches := cl.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		log.Println(errs)
	}
	fetches.EachRecord(func(r *kgo.Record) {
		fmt.Println(string(r.Key), r.Partition, r.Offset)
	})
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		log.Println("commit:", err)
	}
}
```

## 6.8 Rebalance

**Rebalance** partitionlarni guruhdagi consumerlar o'rtasida qayta taqsimlash. Sabablari:

- guruhga yangi consumer qo'shildi;
- consumer to'g'ri tartibda chiqdi yoki yiqildi;
- consumer `session.timeout.ms` dan uzoqroq heartbeat yubormadi;
- consumer `max.poll.interval.ms` dan uzoqroq `poll()` ni chaqirmadi;
- partitionlar soni yoki obuna o'zgardi.

### Klassik protokol: eager va cooperative

```text
EAGER (stop-the-world):
  barcha consumerlar HAMMA partitionlarni topshiradi -> pauza -> yangi taqsimot

COOPERATIVE (incremental, CooperativeStickyAssignor):
  faqat ko'chadigan partitionlar topshiriladi -> qolganlari ishlashda davom etadi
```

| Parametr | Standart qiymat | Ma'nosi |
|---|---|---|
| `session.timeout.ms` | 45000 | Heartbeat bundan uzoqroq kelmasa, consumer o'lgan hisoblanadi |
| `heartbeat.interval.ms` | 3000 | Heartbeat qanchalik tez-tez yuboriladi |
| `max.poll.interval.ms` | 300000 | `poll()` chaqiruvlari orasidagi maksimal vaqt |
| `max.poll.records` | 500 | Bitta `poll()` da keladigan yozuvlarning maksimal soni |
| `partition.assignment.strategy` | `RangeAssignor, CooperativeStickyAssignor` | Tayinlash strategiyasi |

### Eng ko'p uchraydigan avariya: uzoq davom etadigan qayta ishlash

```text
max.poll.records = 500
bitta yozuvni qayta ishlash = 1 s (sekin API chaqiruvi)
500 s > max.poll.interval.ms (300 s)
-> consumer guruhdan chiqariladi
-> rebalance
-> batchni boshqa consumer qaytadan qayta ishlaydi
-> yana ulgurmaydi -> cheksiz rebalance sikli
```

Yechimlar: `max.poll.records` ni kamaytirish, qayta ishlashni tezlashtirish, `max.poll.interval.ms` ni oshirish, og'ir ishni partitionlarni pauza qilgan holda (`consumer.pause()`) threadlar puliga chiqarish.

### Static membership

`group.instance.id` bilan consumer doimiy identifikatsiyaga ega bo'ladi. Kubernetesda pod qayta ishga tushganda, consumer `session.timeout.ms` ichida qaytsa, rebalance boshlanmaydi. Bu rolling deploy paytidagi rebalance bo'ronidan qutqaradi.

## 6.9 Consumer groupning yangi protokoli (KIP-848)

Kafka 4.0 da iste'molchilar **guruhlarining yangi protokoli** hamma uchun ochiq (GA) bo'ldi.

```properties
group.protocol=consumer
```

Nima o'zgardi:

- **partitionlarni tayinlashni** klientdagi guruh leaderi emas, **broker hisoblaydi** (group coordinator);
- rebalance **to'liq inkremental**, barcha ishtirokchilarni global sinxronlashsiz;
- sekin consumer endi butun guruhning rebalanceini sekinlashtirmaydi;
- `session.timeout.ms` va `heartbeat.interval.ms` brokerda sozlanadi (`group.consumer.session.timeout.ms`, `group.consumer.heartbeat.interval.ms`);
- strategiya `partition.assignment.strategy` o'rniga `group.remote.assignor` (`uniform` yoki `range`) orqali tanlanadi.

Kafka 4.x dagi yangi ilovalar uchun `group.protocol=consumer` dan foydalanish tavsiya etiladi. Klassik protokol (`group.protocol=classic`) moslik uchun qoladi.

## 6.10 Consumer lag

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --describe --group billing
```

```text
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID
billing  orders  0          1200            1250            50    consumer-1-...
billing  orders  1          980             4980            4000  consumer-2-...
```

- **LAG** = guruh hali nechta xabarni qayta ishlamagani.
- Yuqori lag **har doim ham muammo emas**: muhimi, u o'syaptimi yoki qisqaryaptimi va kechikish SLAga sig'yaptimi.
- Faqat bitta partitiondagi lag odatda qaynoq kalit yoki «zaharli» xabar borligini bildiradi.

## 6.11 Offsetlarni reset qilish

Topicni boshidan qayta o'qish (guruh to'xtatilgan bo'lishi kerak):

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --group billing --topic orders \
  --reset-offsets --to-earliest --execute
```

Boshqa variantlar: `--to-latest`, `--to-offset 100`, `--shift-by -500`, `--to-datetime 2026-09-15T00:00:00.000`, `--by-duration PT2H`. `--execute` bo'lmasa, buyruq faqat rejani ko'rsatadi (dry run).

### Amaliyot

1. 6 ta partitionli topic uchun `billing` guruhida 3 ta consumerni ishga tushiring va taqsimotni ko'ring.
2. 7-consumerni ishga tushiring va uning bo'sh turganiga ishonch hosil qiling.
3. Qayta ishlashga `Thread.sleep(1000)` qo'shing, `max.poll.interval.ms` ni 10000 gacha kamaytiring va rebalance siklini qayta hosil qiling.
4. Guruh offsetlarini 1 soat orqaga reset qiling.

---

# Modul 7. Exactly-once, Kafka tranzaksiyalari va transactional outbox

## 7.1 Dublikatlar va yo'qotishlar qayerda paydo bo'ladi

| Ssenariy | Natija | Himoya |
|---|---|---|
| Producer ACK olmadi va yuborishni takrorladi | Topicda dublikat | `enable.idempotence=true` |
| Ilova senddan keyin, lekin «yuborildi» belgisini qo'yishdan oldin yiqildi | Biznes-hodisa dublikati | `event_id` + idempotent consumer |
| Consumer qayta ishladi, lekin commitdan oldin yiqildi | Qayta ishlashning takrorlanishi | Idempotent qayta ishlash |
| Consumer qayta ishlashdan oldin commit qildi va yiqildi | Yo'qotish | Qayta ishlashdan keyin commit qilish |
| Servis ma'lumotlar bazasiga yozdi, lekin Kafkaga yuborishdan oldin yiqildi | Hodisaning yo'qolishi | Transactional outbox |
| Servis Kafkaga yubordi, lekin ma'lumotlar bazasi tranzaksiyasi rollback bo'ldi | «Fantom» hodisa | Transactional outbox |
| RF=1 yoki `acks=1` va broker yiqildi | Yo'qotish | RF=3, `min.insync.replicas=2`, `acks=all` |

## 7.2 Kafka tranzaksiyalari

Tranzaksiyalar xabarlarni bir nechta partitionga **atomar** yozish va consumer offsetlarini commit qilish imkonini beradi: yo hammasi, yo hech narsa. Bu exactly-once bilan ishlaydigan **consume-transform-produce** patternining asosi.

```text
      read                    process                   write + commit offsets
orders ----> [   ilova    ] -----------> payments  (bitta Kafka tranzaksiyasi)
```

Bu qanday tuzilgan:

- producer `transactional.id` oladi (nusxaning barqaror identifikatori);
- brokerdagi **Transaction Coordinator** holatni `__transaction_state` topicida saqlaydi;
- partitionlarga ma'lumotlar yoziladi, tranzaksiya oxirida esa **control records** (commit yoki abort);
- `isolation.level=read_committed` bo'lgan consumer faqat commit qilingan ma'lumotlarni ko'radi;
- **fencing**: agar xuddi shu `transactional.id` bilan yangi nusxa ishga tushsa, eski «zombi» `ProducerFencedException` oladi.

```java
Properties p = new Properties();
p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "payment-processor-1");
p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
KafkaProducer<String, String> producer = new KafkaProducer<>(p);
producer.initTransactions();

Properties c = new Properties();
c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
c.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-processor");
c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c);
consumer.subscribe(List.of("orders"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    if (records.isEmpty()) continue;

    producer.beginTransaction();
    try {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> r : records) {
            producer.send(new ProducerRecord<>("payments", r.key(), transform(r.value())));
            offsets.put(new TopicPartition(r.topic(), r.partition()),
                        new OffsetAndMetadata(r.offset() + 1));
        }
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
    } catch (ProducerFencedException e) {
        producer.close();   // bizni boshqa nusxa almashtirdi
        break;
    } catch (KafkaException e) {
        producer.abortTransaction();
        // consumer pozitsiyasini oxirgi commit qilingan offsetga qaytarish
        for (TopicPartition tp : records.partitions()) {
            OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
            consumer.seek(tp, committed == null ? 0 : committed.offset());
        }
    }
}
```

> **Exactly-once chegarasi.** Kafka tranzaksiyalari exactly-once ni **faqat Kafka ichida** beradi: topicdan o'qish, topicga yozish, offsetni commit qilish. Agar qayta ishlash email yuborsa, tashqi APIda pul yechsa yoki PostgreSQLga yozsa, bu effektlar Kafka tranzaksiyasiga kirmaydi. Ular uchun idempotentlik kerak.

## 7.3 Idempotent consumer

Tashqi ma'lumotlar bazasiga yozishda «amalda aynan bir marta» natijasiga erishishning eng ishonchli usuli:

```sql
CREATE TABLE processed_events (
    event_id   UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
BEGIN;
INSERT INTO processed_events (event_id) VALUES ($1)
  ON CONFLICT (event_id) DO NOTHING;
-- agar 0 ta satr qo'shilgan bo'lsa, hodisa allaqachon qayta ishlangan: COMMIT qilib chiqamiz
UPDATE accounts SET balance = balance - $2 WHERE id = $3;
COMMIT;
-- keyin Kafkada offsetni commit qilamiz
```

Boshqa variantlar: tabiiy kalit bo'yicha `UPSERT`, versiya bo'yicha shartli yangilash (`WHERE version = $expected`), partition offsetini natija bilan bitta jadvalda saqlash.

## 7.4 Transactional outbox: hodisani qanday yo'qotmaslik kerak

**Ikki joyga yozish muammosi (dual write):**

```text
1. INSERT INTO orders ...     OK
2. producer.send(OrderCreated) -> servis yiqildi
   -> buyurtma bor, hodisa yo'q, boshqa tizimlar buyurtma haqida bilmadi
```

**Yechim: xuddi shu ma'lumotlar bazasidagi outbox-jadval.**

```text
+------ bitta ma'lumotlar bazasi tranzaksiyasi -------+
| INSERT INTO orders (...)                            |
| INSERT INTO outbox (id, aggregate_id, type, payload)|
+-----------------------------------------------------+
               |
               v
  Debezium (CDC) yoki outbox-relay outboxni o'qiydi
               |
               v
         Kafka topic shop.orders.events.v1
```

```sql
CREATE TABLE outbox (
    id             UUID PRIMARY KEY,
    aggregatetype  TEXT NOT NULL,     -- "order"
    aggregateid    TEXT NOT NULL,     -- Kafka kaliti
    type           TEXT NOT NULL,     -- "OrderCreated"
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Relay hodisani qayta e'lon qilishi mumkin, shuning uchun iste'molchilar baribir `id` bo'yicha idempotent bo'lishi kerak. Debezium Outbox Event Router sozlamasini [11-modulda](#modul-11-kafka-connect-va-debezium-bilan-cdc) ko'ring.

## 7.5 Xulosa: qaysi kafolatni tanlash kerak

| Vazifa | Tavsiya |
|---|---|
| Loglar, metrikalar, kliklar | At-least-once, dublikatlarga yo'l qo'yiladi |
| Mikroservislar, biznes-hodisalar | At-least-once + idempotent consumer + outbox |
| Kafka -> qayta ishlash -> Kafka | Kafka tranzaksiyalari yoki Kafka Streams `exactly_once_v2` |
| Kafka -> ma'lumotlar bazasi | Idempotent yozish yoki offsetni ma'lumotlar bazasining o'sha tranzaksiyasida saqlash |

### O'z-o'zini tekshirish uchun savollar

1. Nima uchun idempotent producer ilova qayta ishga tushgandagi dublikatlardan himoya qilmaydi?
2. Fencing nima va `transactional.id` nima uchun kerak?
3. Nima uchun Kafkadagi exactly-once tashqi HTTP chaqiruviga taalluqli emas?
4. Transactional outbox qanday muammoni hal qiladi?

---

# Modul 8. Ma'lumotlarni saqlash: segmentlar, retention, log compaction

## 8.1 Segmentlar va indekslar

Partition diskda **segmentlarga** bo'lingan:

```text
orders-0/
  00000000000000000000.log        eski segment (yopilgan)
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000001000000.log        eski segment (yopilgan)
  00000000000001000000.index
  00000000000002000000.log        ACTIVE segment: yozish shu yerga ketadi
  00000000000002000000.index
```

- Fayl nomi segmentdagi birinchi yozuvning **base offset**i.
- Yangi segment `segment.bytes` (1 GB) yoki `segment.ms` (7 kun) ga yetganda yaratiladi.
- **O'chirish va kompaktlash faqat yopilgan segmentlar bilan ishlaydi.**
- `.index` siyrak indeks: offset -> fayldagi pozitsiya; yozuvni qidirish: indeks bo'yicha binar qidiruv + qisqa ketma-ket o'qish.

Segment tarkibini ko'rish:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kraft-combined-logs/orders-0/00000000000000000000.log \
  --print-data-log
```

## 8.2 Nima uchun Kafka bunchalik tez

| Usul | Qanday ishlaydi |
|---|---|
| **Ketma-ket yozish** | Faqat fayl oxiriga append, disklar va SSDlar ketma-ket I/O ni yaxshi ko'radi |
| **OS page cache** | Kafka ma'lumotlarni JVM heapda keshlamaydi, o'qish fayl tizimi keshidan bajariladi |
| **Zero-copy** (`sendfile`) | Page cachedagi ma'lumotlar user spacega nusxalanmasdan soketga yuboriladi |
| **Batching** | Yozuvlar producerda, brokerda va consumerda guruhlanadi |
| **Batchlarni siqish** | Tarmoq va disk kamroq sarflanadi, broker ma'lumotlarni ochmaydi |
| **Partitsiyalash** | Yuklama brokerlar va disklar bo'yicha taqsimlangan |
| **Binar protokol** | TCP ustidagi ixcham, pipelining bilan ishlaydigan protokol |

> TLS yoqilganda zero-copy ishlamaydi: ma'lumotlarni user spaceda shifrlash kerak. Shuning uchun TLS brokerlarning CPU yuklamasini sezilarli oshiradi.

## 8.3 Page cache va JVM heap

```text
Server 64 GB RAM
  Kafka JVM heap:  6 GB   (metadata, so'rov buferlari)
  OS page cache:  ~55 GB  (partitionlarning qaynoq ma'lumotlari)
```

Tavsiyalar:

- brokerning heapi uchun 4-8 GB katta yuklamalarda ham yetarli;
- qolgan xotirani operatsion tizimga page cache uchun qoldiring;
- `vm.swappiness=1`, brokerdagi swap kechikishlarni keskin yomonlashtiradi;
- topicning «dumini» o'qiydigan consumerlar ma'lumotni xotiradan oladi; eski tarixni o'qiydigan consumerlar diskka boradi va qaynoq ma'lumotlarni siqib chiqarishi mumkin.

## 8.4 Retention: qancha saqlash kerak

| Parametr | Standart qiymat | Ma'nosi |
|---|---|---|
| `retention.ms` (topic) / `log.retention.hours` (broker) | 7 kun | Saqlash vaqti |
| `retention.bytes` | `-1` (limitsiz) | **Har bir partition uchun** o'lcham limiti |
| `segment.bytes` | 1 GB | Segment o'lchami |
| `segment.ms` | 7 kun | Faol segmentning maksimal yoshi |

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=259200000
```

> Tuzoq: trafik kam bo'lganda faol segment haftalab yopilmasligi mumkin va ma'lumotlar retentiondan uzoqroq saqlanadi. Bunday topiclar uchun `segment.ms` ni kamaytiring.

## 8.5 cleanup.policy: delete va compact

**`delete`** (standart qiymat) retentiondan eski bo'lgan segmentlarni butunligicha o'chiradi.

**`compact`** **har bir kalit uchun oxirgi qiymatni** qoldiradi:

```text
Kompaktlashdan oldin:
offset 0: user-1 -> {"name":"Ann"}
offset 1: user-2 -> {"name":"Bob"}
offset 2: user-1 -> {"name":"Anna"}
offset 3: user-2 -> null            <- tombstone (kalitni o'chirish)
offset 4: user-3 -> {"name":"Kate"}

Kompaktlashdan keyin:
offset 2: user-1 -> {"name":"Anna"}
offset 4: user-3 -> {"name":"Kate"}
(user-2 uchun tombstone delete.retention.ms dan keyin o'chiriladi)
```

Log compaction qayerda ishlatiladi:

- `__consumer_offsets` va boshqa ichki topiclar;
- holat snapshotlari: foydalanuvchi profillari, narxlar, qoldiqlar, sozlamalar;
- CDC-topiclar (jadval satrining oxirgi holati);
- Kafka Streamsdagi state storelarning changelog-topiclari.

Kompaktlash sozlamalari: `min.cleanable.dirty.ratio` (0.5), `min.compaction.lag.ms`, `max.compaction.lag.ms`, `delete.retention.ms` (1 kun). Birlashtirish mumkin: `cleanup.policy=compact,delete`.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --topic user-profile \
  --partitions 6 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=60000
```

## 8.6 Tiered Storage

**Tiered Storage** (KIP-405, Kafka 3.9 dan production-ready) yopilgan segmentlarni obyekt omboriga (S3, GCS, Azure Blob, MinIO) chiqaradi, brokerning lokal disklarida esa faqat «qaynoq» dumni saqlaydi.

```text
broker local disk:  oxirgi 1-2 kun (tez)
object storage:     oylar va yillar (arzon)
```

Nima beradi: arzon uzoq muddatli saqlash, kichikroq disklar, brokerlarni tez qayta ishga tushirish va qayta balanslash. Brokerda `remote.log.storage.system.enable=true`, topicda `remote.storage.enable=true` bilan yoqiladi, bunga qo'shimcha muayyan ombor uchun RemoteStorageManager plagini kerak.

### O'z-o'zini tekshirish uchun savollar

1. Nima uchun Kafka disk bilan samarali ishlaydi va ma'lumotlarni JVM heapda saqlamaydi?
2. Nima uchun yozuvlar kam keladigan topicda retention ishlamay qolishi mumkin?
3. Qachon `delete` o'rniga `compact` ni tanlash kerak?
4. Tombstone nima?

---

# Modul 9. Share Groups: Kafkadagi navbatlar

## 9.1 Share Groups qanday muammoni hal qiladi

Oddiy consumer groupda parallellik partitionlar soni bilan cheklangan. Vazifalar navbati (xat yuborish, PDF generatsiya qilish, tashqi APIlarni chaqirish) uchun bu noqulay: 6 ta partitionli topicga 100 ta worker qo'ygingiz keladi.

**Share Groups** (KIP-932 «Queues for Kafka», Kafka 4.2 dan hamma uchun ochiq) quyidagilarga imkon beradi:

- bir nechta consumer **bitta partitionni bir vaqtda o'qiydi**;
- **har bir yozuv alohida** tasdiqlanadi;
- tasdiqlanmagan yozuvlar avtomatik **qayta yetkaziladi**;
- yetkazish urinishlari soni cheklanadi.

```text
Consumer group:  partition-0 -> aynan 1 ta consumer
Share group:     partition-0 -> consumer-1, consumer-2, ... consumer-N
```

## 9.2 Bu qanday ishlaydi

- broker yozuvlarni consumerga **vaqtinchalik blokirovka (acquisition lock)** bilan beradi, standart qiymati 30 soniya;
- consumer yozuvni tasdiqlaydi: `ACCEPT` (qayta ishlandi), `RELEASE` (navbatga qaytarish), `REJECT` (qayta ishlab bo'lmaydigan deb tashlab yuborish);
- agar blokirovka tasdiqsiz tugasa, yozuv boshqa consumerga yetkaziladi;
- urinishlar limiti (`group.share.delivery.count.limit`, standart qiymati 5) oshib ketgach, yozuv qayta ishlab bo'lmaydigan hisoblanadi;
- **tartib kafolatlanmaydi**, bu parallellik uchun ongli ravishda qilingan murosa.

## 9.3 Javadagi misol

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:19092");
props.put("group.id", "email-workers");
props.put("key.deserializer", StringDeserializer.class.getName());
props.put("value.deserializer", StringDeserializer.class.getName());
props.put("share.acknowledgement.mode", "explicit");

try (KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props)) {
    consumer.subscribe(List.of("email-jobs"));
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
            try {
                sendEmail(r.value());
                consumer.acknowledge(r, AcknowledgeType.ACCEPT);
            } catch (TemporaryException e) {
                consumer.acknowledge(r, AcknowledgeType.RELEASE); // keyinroq takrorlash
            } catch (Exception e) {
                consumer.acknowledge(r, AcknowledgeType.REJECT);  // takrorlamaslik
            }
        }
        consumer.commitSync();
    }
}
```

Tajribalar uchun konsol klienti:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-share-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic email-jobs --group email-workers
```

Agar klasteringizda share groups o'chirilgan bo'lsa, ular feature flag orqali yoqiladi:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server kafka-1:9092 upgrade --feature share.version=1
```

## 9.4 Consumer group yoki share group

| Nima kerak | Tanlov |
|---|---|
| Hodisalarning kalit bo'yicha tartibi | Consumer group |
| Stream processing, agregatlar, CDC | Consumer group |
| Mustaqil vazifalar navbati, workerlar | Share group |
| Partitionlar sonidan katta parallellik | Share group |
| Partitionni bloklamasdan har bir yozuvni alohida retry qilish | Share group |

---

# Modul 10. Schema Registry, Avro, Protobuf va hodisalarni loyihalash

## 10.1 Kafka baytlarni saqlasa, sxemalar nima uchun kerak

Kafka xabarlar tarkibini tekshirmaydi. Kontrakt bo'lmasa, ertami-kechmi shunday bo'ladi:

```text
Order Service maydon nomini o'zgartirdi: amount -> total_amount
   -> 5 ta iste'molchi tungi soat 3 da yiqiladi
```

**Schema Registry** sxemalar versiyalarini saqlaydi va yangi versiya e'lon qilinayotganda moslikni tekshiradi. Implementatsiyalari: Confluent Schema Registry, Apicurio Registry, Karapace.

```text
producer --(sxemani ro'yxatdan o'tkazadi, schema id oladi)--> Schema Registry
producer --[magic byte][schema id][payload]--> Kafka
consumer --(schema id bo'yicha sxemani oladi)--> Schema Registry
```

## 10.2 Xabar formatlari

| Format | Afzalliklari | Kamchiliklari | Qachon tanlash kerak |
|---|---|---|---|
| **JSON** | O'qish oson, debug qilish sodda | O'lchami katta, qat'iy sxema yo'q | Prototiplar, kichik yuklamalar |
| **JSON Schema** | JSON + validatsiya | O'lchami JSON bilan bir xil | JSON ham, kontrakt ham kerak bo'lganda |
| **Avro** | Ixcham, sxemalar evolyutsiyasi a'lo darajada | Sxemalar reyestri kerak | Data-platformalar, CDC, analitika |
| **Protobuf** | Ixcham, kod generatsiyasi, gRPC ekotizimi | Evolyutsiya qoidalariga rioya qilish kerak | Mikroservislar, turli tillarda yozadigan (polyglot) jamoalar |

## 10.3 Sxemalarning moslik rejimlari

| Rejim | Nimaga ruxsat berilgan | Kimni birinchi yangilash kerak |
|---|---|---|
| `BACKWARD` (ko'pincha standart qiymat) | Yangi sxema eski ma'lumotlarni o'qiydi: maydonni o'chirish, defaultli maydon qo'shish | Consumer |
| `FORWARD` | Eski sxema yangi ma'lumotlarni o'qiydi: maydon qo'shish, defaultli maydonni o'chirish | Producer |
| `FULL` | Ikkalasi ham | Istalgan tartibda |
| `*_TRANSITIVE` | **Barcha** oldingi versiyalarga nisbatan tekshirish | |
| `NONE` | Tekshiruvsiz | Productionda ishlatmang |

Avro-sxema misoli:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "shop.orders.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "currency", "type": "string", "default": "RUB"},
    {"name": "promo_code", "type": ["null", "string"], "default": null}
  ]
}
```

**Xavfsiz evolyutsiya qoidalari:**

- yangi maydonlarni faqat `default` bilan qo'shing;
- maydonlar nomini o'zgartirmang, yangisini qo'shing va eskisini eskirgan deb belgilang;
- maydon turini o'zgartirmang;
- buzuvchi o'zgarish (breaking change) = yangi topic yoki yangi hodisa turi (`v2`).

## 10.4 Hodisalarni loyihalash

**Yaxshi hodisa:**

```json
{
  "event_id": "b3c9...",
  "event_type": "OrderPaid",
  "event_version": 2,
  "occurred_at": "2026-09-15T10:21:43Z",
  "source": "payment-service",
  "trace_id": "4bf92f3577b34da6",
  "data": {
    "order_id": "order-123",
    "payment_id": "pay-987",
    "amount": 4990,
    "currency": "RUB"
  }
}
```

| Savol | Tavsiya |
|---|---|
| Har bir hodisa turiga bitta topicmi yoki har bir obyektgami? | Hayot sikli tartibi uchun: har bir agregatga bitta topic (`orders`), ichida turli hodisa turlari |
| Semiz hodisami yoki ozg'in hodisami? | Event-carried state transfer (barcha kerakli ma'lumotlar) ortga qaytuvchi HTTP chaqiruvlarini kamaytiradi; ozg'in hodisa soddaroq, lekin bog'liqlik tug'diradi |
| Pul | Minimal birliklardagi butun sonlar (tiyin) + valyuta, float emas |
| Vaqt | UTC, ISO-8601 yoki epoch millis |
| Metadata | Headerlarda: `trace_id`, `event_type`, `content-type` |
| Shaxsiy ma'lumotlar | Minimallashtiring, compacted-topiclarda tombstone orqali o'chiring |

**CloudEvents** standarti hodisalar metadatasining umumiy formatini tavsiflaydi va kompaniyadagi kelishuvlarga asos bo'lishi mumkin.

---

# Modul 11. Kafka Connect va Debezium bilan CDC

## 11.1 Kafka Connect nima

**Kafka Connect** Kafkani tashqi tizimlar bilan **kod yozmasdan** integratsiya qilish uchun freymvork.

```text
PostgreSQL --[Source connector]--> Kafka --[Sink connector]--> Elasticsearch
MySQL                                                         S3 / ClickHouse
MongoDB                                                       Snowflake / JDBC
```

| Tushuncha | Ma'nosi |
|---|---|
| **Source connector** | Tashqi tizimdan o'qiydi va Kafkaga yozadi |
| **Sink connector** | Kafkadan o'qiydi va tashqi tizimga yozadi |
| **Worker** | Connectning JVM-jarayoni; distributed rejimda workerlar klaster hosil qiladi |
| **Task** | Connectorning parallellik birligi |
| **Converter** | Serializatsiya: `JsonConverter`, `AvroConverter`, `ProtobufConverter` |
| **SMT** (Single Message Transform) | Yozuvni yengil o'zgartirish: maydon nomini o'zgartirish, kalitni ajratib olish, marshrutlash |

Distributed Connectning holati Kafkada saqlanadi: `connect-configs`, `connect-offsets`, `connect-status` topiclari.

## 11.2 CDC: Change Data Capture

**CDC** ma'lumotlar bazasidagi o'zgarishlarni hodisalar oqimiga aylantiradi. **Debezium** tranzaksiyalar jurnalini (PostgreSQLda WAL, MySQLda binlog, MongoDBda oplog) o'qiydi va har bir `INSERT`, `UPDATE`, `DELETE` ni Kafkaga e'lon qiladi.

```text
UPDATE customers SET email='new@mail.ru' WHERE id=42;
        |
        v  (WAL / logical replication)
Debezium PostgreSQL connector
        |
        v
topic: crm.public.customers
{
  "before": {"id": 42, "email": "old@mail.ru"},
  "after":  {"id": 42, "email": "new@mail.ru"},
  "op": "u",
  "source": {"lsn": 123456789, "table": "customers"},
  "ts_ms": 1789460503000
}
```

Jadvalni so'rov bilan tekshirib turishga (`SELECT ... WHERE updated_at > ?`) nisbatan afzalliklari: o'chirishlar ko'rinadi, tez-tez so'rovlardan yuklama yo'q, barcha oraliq o'zgarishlar keladi, kechikish minimal.

## 11.3 Misol: Debezium PostgreSQL connector

PostgreSQLni tayyorlash: `wal_level=logical`, `REPLICATION` huquqiga ega foydalanuvchi.

Connectorni Kafka Connect REST API orqali ro'yxatdan o'tkazish:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "crm-postgres-cdc",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "${file:/secrets/db.properties:password}",
      "database.dbname": "crm",
      "topic.prefix": "crm",
      "plugin.name": "pgoutput",
      "table.include.list": "public.customers,public.orders",
      "snapshot.mode": "initial",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
  }'
```

REST APIning foydali buyruqlari:

```bash
curl http://localhost:8083/connectors                              # ro'yxat
curl http://localhost:8083/connectors/crm-postgres-cdc/status      # tasklar holati
curl -X POST http://localhost:8083/connectors/crm-postgres-cdc/restart?includeTasks=true
curl -X PUT  http://localhost:8083/connectors/crm-postgres-cdc/pause
```

## 11.4 Debezium Event Router orqali outbox

```json
{
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.key": "aggregateid",
  "transforms.outbox.route.by.field": "aggregatetype",
  "transforms.outbox.route.topic.replacement": "shop.${routedByValue}.events.v1",
  "table.include.list": "public.outbox"
}
```

`outbox` dagi `aggregatetype=order` bo'lgan yozuv `shop.order.events.v1` topiciga `aggregateid` kaliti bilan tushadi.

## 11.5 Kafka Connectdagi xatolar

```properties
errors.tolerance=all
errors.deadletterqueue.topic.name=dlq.crm-sink
errors.deadletterqueue.context.headers.enable=true
errors.log.enable=true
```

Connectda dead letter queue **sink**-connectorlar uchun ishlaydi: buzuq yozuv DLQga ketadi, connector esa ishni davom ettiradi.

**CDCning tipik muammolari:** connector to'xtatilganda PostgreSQLda o'sib boruvchi replication slot (WAL diskni to'ldiradi), jadval sxemasining o'zgarishi, katta jadvallarning uzoq davom etadigan initial snapshoti, `UPDATE`/`DELETE` da `before` ni olish uchun `REPLICA IDENTITY`.

---

# Modul 12. Kafka Streams: ma'lumotlar oqimini qayta ishlash

## 12.1 Kafka Streams nima

**Kafka Streams** stream processing uchun Java-kutubxona. U **alohida klasterni talab qilmaydi**: ilova oddiy JAR bo'lib, qo'shimcha nusxalarni ishga tushirish orqali masshtablanadi.

```text
orders topic --> [Kafka Streams app x3 instances] --> orders-per-minute topic
                        |
                   state store (RocksDB) + Kafkadagi changelog topic
```

Muqobillar: **Apache Flink** (kuchli alohida klaster, SQL, murakkab oynalar), **ksqlDB** (Kafka Streams ustidagi SQL), **Spark Structured Streaming**.

## 12.2 KStream, KTable, GlobalKTable

| Abstraksiya | Ma'nosi | Misol |
|---|---|---|
| **KStream** | Mustaqil hodisalar oqimi (insert) | Kliklar, to'lovlar |
| **KTable** | Changelog: kalit bo'yicha oxirgi qiymat (upsert) | Foydalanuvchining joriy profili |
| **GlobalKTable** | Har bir nusxaga to'liq replikatsiya qilingan KTable | Kichik ma'lumotnomalar |

**Oqim va jadvalning ikki yoqlamaligi:** o'zgarishlar oqimini jadvalga yig'ish mumkin, jadvalni esa qaytadan o'zgarishlar oqimiga yoyish mumkin.

## 12.3 Misol: foydalanuvchilar bo'yicha tushum va daqiqadagi buyurtmalar

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-analytics");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> orders = builder.stream("orders");

// 1. Daqiqadagi buyurtmalar soni (tumbling window)
orders
    .groupBy((key, value) -> "all")
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .count(Materialized.as("orders-per-minute-store"))
    .toStream()
    .map((windowedKey, count) -> KeyValue.pair(windowedKey.window().startTime().toString(), count.toString()))
    .to("orders-per-minute");

// 2. Foydalanuvchi bo'yicha buyurtmalar summasi (KTable)
orders
    .selectKey((key, value) -> extractUserId(value))
    .mapValues(value -> extractAmount(value))
    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
    .reduce(Long::sum, Materialized.as("revenue-by-user-store"))
    .toStream()
    .to("revenue-by-user", Produced.with(Serdes.String(), Serdes.Long()));

KafkaStreams streams = new KafkaStreams(builder.build(), props);
Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
streams.start();
```

## 12.4 Kafka Streamsning asosiy konsepsiyalari

| Konsepsiya | Nimani bilish muhim |
|---|---|
| **Stateless-operatsiyalar** | `filter`, `map`, `flatMap`, `branch`: ombor talab qilmaydi |
| **Stateful-operatsiyalar** | `count`, `aggregate`, `reduce`, `join`: holat RocksDBda + changelog-topic |
| **Repartition** | Yangi kalit bo'yicha `selectKey`/`groupBy` ichki repartition-topic yaratadi |
| **Oynalar** | Tumbling, hopping, sliding, session |
| **Grace period** | Oyna yopilgandan keyin kechikkan hodisalarni qancha kutish |
| **Event time** | Kelgan vaqti emas, hodisa vaqti bo'yicha qayta ishlash |
| **Joins** | Stream-stream (oyna ichida), stream-table (boyitish), table-table |
| **Co-partitioning** | Join uchun topiclarning partitionlar soni ham, kalitlari ham bir xil bo'lishi kerak |
| **Interactive queries** | State storeni REST orqali to'g'ridan-to'g'ri ilovadan o'qish |
| **Standby replicas** | `num.standby.replicas` yiqilishdan keyin holatni tiklashni tezlashtiradi |

---

# Modul 13. Xatolarni qayta ishlash: retry, DLQ va poison pill

## 13.1 Xato turlari

| Tur | Misol | Nima qilish kerak |
|---|---|---|
| **Vaqtinchalik** (transient) | Ma'lumotlar bazasi timeouti, tashqi APIdan 503 | Backoff bilan takrorlash |
| **Doimiy** (permanent) | Yaroqsiz JSON, biznes-qoidaning buzilishi | DLQga yuborish, takrorlamaslik |
| **Poison pill** | Deserializatorni har doim yiqitadigan xabar | DLQ, aks holda partition butunlay bloklanadi |

## 13.2 Nima uchun consumerda cheksiz takrorlab bo'lmaydi

```text
partition-3: [msg 100 - buzuq] [101] [102] [103] ...
consumer 100 ni cheksiz retry qiladi
-> partition-3 ning barcha hodisalari to'xtab turadi
-> lag o'sadi, mijozlarning buyurtmalari qayta ishlanmaydi
```

## 13.3 Retry-topiclar va DLQ patterni

```text
orders ---> main consumer --xato----> orders.retry.30s ---> retry consumer (30 s kutadi)
                                           |
                                         xato
                                           v
                                    orders.retry.5m ---> retry consumer (5 min kutadi)
                                           |
                                         xato
                                           v
                                      orders.dlq  ---> alert, qo'lda tahlil, qayta yuborish
```

Qoidalar:

- headerlarda `original-topic`, `original-partition`, `original-offset`, `attempt`, `error-class`, `error-message`, `failed-at` ni saqlang;
- retry-consumer `poll`-siklda uxlamaydi, balki takrorlash vaqti kelguncha partitionni pauzaga qo'yadi (`pause`/`resume`);
- **retry-topiclar tartibni buzadi**; agar kalit bo'yicha tartib juda muhim bo'lsa, muvaffaqiyatli qayta ishlanguncha kalitni bloklang yoki retrylarni cheklov bilan joyida bajaring;
- DLQga albatta **alert** va **qayta yuborish** (redrive) vositasini o'rnating;
- Spring for Apache Kafkada tayyor implementatsiya bor: `@RetryableTopic` va `DeadLetterPublishingRecoverer` bilan `DefaultErrorHandler`.

## 13.4 Deserializatsiya xatolari

Java-klientda deserializatsiya exceptioni `poll()` dan tashlanadi va partitionni bloklaydi. Yechimlar:

- `ErrorHandlingDeserializer` wrapperi (Spring Kafka);
- `byte[]` ni o'qib, kodda `try/catch` bilan deserializatsiya qilish;
- Kafka Streamsda: `deserialization.exception.handler=LogAndContinueExceptionHandler` yoki DLQga yuboradigan o'z handleringiz.

---

# Modul 14. Kafka unumdorligi va tuning

## 14.1 Throughput va latency

```text
                kattaroq batchlar, kattaroq linger.ms, siqish
throughput  <----------------------------------------->  latency
                kichik batchlar, linger.ms=0, acks=1
```

Avval maqsadni aniqlang: analitika uchun soniyasiga millionlab hodisami yoki savdo tizimi uchun p99 < 10 ms mi. Ikkalasining maksimumiga bir vaqtda erishib bo'lmaydi.

## 14.2 Producer tuningi

| Parametr | Throughput uchun | Latency uchun |
|---|---|---|
| `linger.ms` | 20-100 | 0-5 |
| `batch.size` | 128 KB - 1 MB | 16-32 KB |
| `compression.type` | `lz4`, `zstd` | `none`, `lz4` |
| `buffer.memory` | 64-256 MB | Standart qiymat |
| `acks` | `all` (odatda ishonchlilik muhimroq) | `all` yoki `1` |

## 14.3 Consumer tuningi

| Parametr | Standart qiymat | Throughput uchun |
|---|---|---|
| `fetch.min.bytes` | 1 | 64 KB - 1 MB: broker javob berishdan oldin ma'lumot to'playdi |
| `fetch.max.wait.ms` | 500 | `fetch.min.bytes` bo'lganda kutishning yuqori chegarasi |
| `max.partition.fetch.bytes` | 1 MB | Yirik xabarlar uchun kattaroq |
| `fetch.max.bytes` | 50 MB | Fetch javobining limiti |
| `max.poll.records` | 500 | Qayta ishlash tez bo'lsa kattaroq |

Ko'pincha tor joy (bottleneck) Kafka emas, balki **consumerdagi qayta ishlash** bo'ladi: ma'lumotlar bazasiga har bir yozuv uchun alohida sinxron chaqiruvlar. Paketli yozishdan (batch insert), kalitlar bo'yicha parallel qayta ishlashdan, asinxron klientlardan foydalaning.

## 14.4 Broker tuningi

```properties
num.network.threads=6          # tarmoq so'rovlari threadlari (standart 3)
num.io.threads=16              # so'rovlar/diskni qayta ishlash threadlari (standart 8)
num.replica.fetchers=4         # parallel replikatsiya (standart 1)
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
message.max.bytes=1048588      # batchning maksimal o'lchami (~1 MB)
auto.create.topics.enable=false
```

`log.flush.interval.*` ga tegmang: Kafka har bir yozuvning fsynciga emas, replikatsiyaga tayanadi.

## 14.5 Operatsion tizim va apparat ta'minoti

| Soha | Tavsiya |
|---|---|
| **Disklar** | NVMe/SSD yoki JBODdagi bir nechta HDD; Kafka ma'lumotlari uchun alohida disklar |
| **Fayl tizimi** | XFS (yoki ext4), `noatime` bilan mount qilish |
| **Xotira** | JVM heap 6 GB, qolgani page cache |
| **Swap** | `vm.swappiness=1` |
| **Limitlar** | `nofile` ≥ 100000 (segmentlar va soketlar ko'p), `vm.max_map_count` ≥ 262144 |
| **Tarmoq** | 10-25 Gbit, replikatsiya va consumerlar ko'pincha diskdan oldin tarmoqqa taqaladi |
| **JVM** | Java 17 yoki 21, G1GC (standart), GC pauzalarini kuzating |
| **CPU** | TLS, siqish va ko'p sonli ulanishlar uchun muhim |

## 14.6 Yuklama testi

```bash
# producer: har biri 1 KB dan 5 mln yozuv
docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf --num-records 5000000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers=kafka-1:9092 acks=all linger.ms=20 batch.size=262144 compression.type=lz4

# consumer
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server kafka-1:9092 --topic perf --messages 5000000 --group perf-test
```

Xabarlarning real o'lchami va formatida, productiondagi bilan bir xil `acks`, TLS va partitionlar soni bilan test qiling.

## 14.7 Katta xabarlar

Kafka ~1 MB gacha bo'lgan xabarlar uchun optimallashtirilgan. Fayllar, rasmlar va katta hujjatlar uchun **claim check pattern** dan foydalaning: obyektni S3/MinIOga qo'ying, Kafkaga esa havola va metadatani yuboring.

---

# Modul 15. Kafka monitoringi: metrikalar, consumer lag, alertlar

## 15.1 Monitoring steki

```text
Kafka brokers (JMX) --> JMX Exporter --> Prometheus --> Grafana
                                            |
Consumer lag -------> kafka-exporter/Burrow +--> Alertmanager --> Telegram/Slack/PagerDuty
Klient metrikalari --> Micrometer ---------->
```

## 15.2 Brokerning asosiy metrikalari

| Metrika (JMX) | Norma | Chetga chiqish nimani bildiradi |
|---|---|---|
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | 0 | Replikalar ortda qolmoqda: broker yiqilgan, disk yoki tarmoq ortiqcha yuklangan |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | 0 | `acks=all` bilan yozish rad etilmoqda |
| `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | 0 | Leadersiz partitionlar: ma'lumotlarga kirib bo'lmaydi |
| `kafka.controller:type=KafkaController,name=ActiveControllerCount` | Klasterga 1 ta | 0 = controller yo'q, >1 = split brain |
| `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` / `IsrExpandsPerSec` | 0 atrofida | Replikalar «miltillayapti»: tarmoq, GC, ortiqcha yuklama |
| `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | > 30% | Broker so'rovlarni qayta ishlashga ulgurmayapti |
| `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | > 30% | Tarmoq threadlari ortiqcha yuklangan |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce` | Barqaror | Yozish kechikishining p99 qiymati o'smoqda |
| `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` / `BytesOutPerSec` | Profil bo'yicha | Trafik va sig'imni rejalashtirish |
| Disk, CPU, tarmoq, GC pause | Zaxira > 30% | Masshtablash zarur |

## 15.3 Klient metrikalari

| Klient | Metrika | Nima uchun |
|---|---|---|
| Producer | `record-error-rate`, `record-retry-rate` | Yuborishdagi xatolar va retrylar |
| Producer | `request-latency-avg`, `batch-size-avg`, `compression-rate-avg` | Batching samaradorligi |
| Producer | `buffer-available-bytes` | Bufer to'lib ketmoqda, `send()` bloklanmoqda |
| Consumer | `records-lag-max` | Ortda qolish |
| Consumer | `commit-rate`, `rebalance-rate-per-hour` | Tez-tez rebalance = muammo |
| Consumer | `last-poll-seconds-ago` | Tiqilib qolgan poll-sikl |

## 15.4 Har kimda bo'lishi kerak bo'lgan alertlar

```yaml
groups:
- name: kafka
  rules:
  - alert: KafkaOfflinePartitions
    expr: sum(kafka_controller_kafkacontroller_offlinepartitionscount) > 0
    for: 1m
    labels: {severity: critical}

  - alert: KafkaUnderMinIsr
    expr: sum(kafka_server_replicamanager_underminisrpartitioncount) > 0
    for: 2m
    labels: {severity: critical}

  - alert: KafkaUnderReplicatedPartitions
    expr: sum(kafka_server_replicamanager_underreplicatedpartitions) > 0
    for: 10m
    labels: {severity: warning}

  - alert: KafkaConsumerLagGrowing
    expr: sum by (consumergroup, topic) (kafka_consumergroup_lag) > 100000
          and deriv(sum by (consumergroup, topic) (kafka_consumergroup_lag)[15m:1m]) > 0
    for: 15m
    labels: {severity: warning}
```

> Metrikalar nomlari JMX Exporter qoidalariga va tanlangan lag exporteriga bog'liq. Ularni o'z konfiguratsiyangiz bilan solishtiring.

**Lag bo'yicha eng yaxshi alert** vaqt bilan ifodalanadi: «lag 100000 ta xabardan ko'p» emas, balki «guruh 5 daqiqadan ko'proq ortda qolmoqda». 100000 ta xabar kliklar uchun soniyalar, to'lovlar uchun esa falokat.

## 15.5 Runbook: under-replicated partitions

1. Barcha URP bitta brokerdami? Broker tirikligini, diskni, GC-loglarni tekshiring.
2. URP barcha brokerlardami? Tarmoqni va umumiy trafikni (`BytesInPerSec`) tekshiring.
3. `IsrShrinksPerSec` o'syaptimi? GC pauzalarini va tarmoqning to'yinishini qidiring.
4. Disk to'lganmi? Muammoli topiclarning retentionini qisqartiring, disk qo'shing, partitionlarni ko'chiring.

---

# Modul 16. Kafka xavfsizligi: TLS, SASL, ACL, kvotalar

## 16.1 Himoyaning uch darajasi

| Daraja | Mexanizm |
|---|---|
| **Shifrlash** | Klientlar va brokerlar o'rtasida, brokerlar o'rtasida, controllerlargacha TLS |
| **Autentifikatsiya** | mTLS, SASL/SCRAM-SHA-512, SASL/OAUTHBEARER (OIDC), SASL/GSSAPI (Kerberos) |
| **Avtorizatsiya** | `StandardAuthorizer` (KRaft) orqali ACL |
| **Kvotalar** | Klient yoki foydalanuvchi uchun bayt/s va so'rovlar limitlari |

Standart holatda Kafka **shifrlashsiz va autentifikatsiyasiz** ishlaydi. Bunday klasterni hech qachon internetga ochmang.

## 16.2 SASL_SSL bilan broker konfiguratsiyasi

```properties
listeners=SASL_SSL://:9094,CONTROLLER://:9093
advertised.listeners=SASL_SSL://kafka-1.prod.internal:9094
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512

ssl.keystore.location=/etc/kafka/ssl/kafka-1.keystore.p12
ssl.keystore.type=PKCS12
ssl.truststore.location=/etc/kafka/ssl/truststore.p12
ssl.truststore.type=PKCS12

authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:admin
```

## 16.3 SCRAM foydalanuvchilari

```bash
kafka-configs.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=CHANGE_ME]' \
  --entity-type users --entity-name order-service
```

Klient konfiguratsiyasi:

```properties
bootstrap.servers=kafka-1.prod.internal:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="order-service" password="${env:KAFKA_PASSWORD}";
ssl.truststore.location=/etc/app/truststore.p12
ssl.truststore.type=PKCS12
```

## 16.4 ACL: minimal imtiyozlar tamoyili

```bash
# producer: orders ga yozish
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --add --allow-principal User:order-service \
  --operation Write --operation Describe --topic shop.orders.events.v1

# idempotent producer
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --add --allow-principal User:order-service --operation IdempotentWrite --cluster

# consumer: topicni o'qish va o'z guruhidan foydalanish
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --add --allow-principal User:billing-service \
  --operation Read --operation Describe --topic shop.orders.events.v1 \
  --group billing --resource-pattern-type literal

# ACL ro'yxati
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties --list
```

Tranzaksion producer uchun `--transactional-id` ga `Write` va `Describe` qo'shing. Prefiksli ACLlar (`--resource-pattern-type prefixed --topic shop.orders.`) domenlar bo'yicha boshqaruvni soddalashtiradi.

## 16.5 Kvotalar

```bash
kafka-configs.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --alter --add-config 'producer_byte_rate=10485760,consumer_byte_rate=20971520,request_percentage=200' \
  --entity-type users --entity-name analytics-service
```

Kvotalar klasterni «shovqinli qo'shni»dan himoya qiladi: bitta jamoaning omadsiz batch-jobi butun kompaniya uchun Kafkani yiqitmasligi kerak.

---

# Modul 17. Kafka productionda: arxitektura va ekspluatatsiya

## 17.1 Klasterning namunaviy arxitekturasi

```text
                 Availability Zone A    Zone B          Zone C
Controllers:     controller-1           controller-2    controller-3
Brokers:         broker-1, broker-4     broker-2, 5     broker-3, 6
                 (broker.rack=a)        (rack=b)        (rack=c)

Topiclar: RF=3, min.insync.replicas=2, replikalar turli zonalarda
Klientlar: acks=all, idempotentlik, o'z zonasidan o'qish uchun client.rack
```

## 17.2 Sig'imni hisoblash (capacity planning)

```text
Kiruvchi trafik:          50 MB/s
Retention:                 7 kun
Replication factor:        3
Zaxira:                    40%

Saqlash = 50 MB/s × 86400 s × 7 × 3 ≈ 90.7 TB
40% zaxira bilan          ≈ 127 TB
Siqish (lz4, JSON uchun ~x3) hajmni kamaytiradi, haqiqiy koeffitsiyentni hisobga oling
```

Yana quyidagilarni tekshiring: chiquvchi tarmoq trafigi (replikatsiya × 2 + barcha consumer grouplar), bitta brokerdagi partitionlar soni (tipik apparat uchun mo'ljal: brokerga 4000 tagacha replika), disk almashtirilgandan keyin brokerning tiklanish vaqti.

## 17.3 Topicni loyihalash cheklisti

- [ ] Kompaniya standartiga mos tushunarli nom
- [ ] Kalit tartibning biznes invariantiga mos tanlangan
- [ ] Partitionlar soni zaxira bilan hisoblangan
- [ ] `replication.factor=3`, `min.insync.replicas=2`
- [ ] Retention iste'molchilar va ma'lumotlarga qo'yilgan talablar bilan kelishilgan
- [ ] `cleanup.policy` ongli ravishda tanlangan
- [ ] Sxema ro'yxatdan o'tkazilgan, moslik rejimi belgilangan
- [ ] Topic egasi va iste'molchilar ro'yxati aniqlangan
- [ ] Muhim iste'molchilar uchun DLQ va alertlar bor
- [ ] ACLlar minimal imtiyozlar tamoyili bo'yicha berilgan
- [ ] Shaxsiy ma'lumotlar minimallashtirilgan

## 17.4 Klaster bilan operatsiyalar

**Rolling restart va versiyani yangilash:** brokerlarni bittadan, keyingisiga o'tishdan oldin `UnderReplicatedPartitions = 0` bo'lishini kutish, controllerlarni alohida yangilash. Binar fayllar yangilangandan keyin `metadata.version` ko'tariladi:

```bash
kafka-features.sh --bootstrap-server kafka-1:9092 describe
kafka-features.sh --bootstrap-server kafka-1:9092 upgrade --release-version 4.3
```

**Partitionlarni** yangi brokerlarga **ko'chirish**:

```bash
kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 \
  --topics-to-move-json-file topics.json --broker-list "4,5,6" --generate

kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 \
  --reassignment-json-file plan.json --execute --throttle 50000000

kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 \
  --reassignment-json-file plan.json --verify
```

Har doim `--throttle` dan foydalaning, aks holda ma'lumotlarni ko'chirish tarmoqni band qilib qo'yadi va production trafigi zarar ko'radi. Avtomatik balanslash uchun **Cruise Control** bor.

## 17.5 Kubernetesda Kafka

- **Strimzi**: Kubernetes uchun eng mashhur open-source Kafka operatori. `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, `KafkaConnect` resurslari, KRaftni qo'llab-quvvatlash.
- Lokal yoki tez tarmoq disklariga ega `StatefulSet`ga o'xshash ombordan, zonalar bo'yicha anti-affinitydan, PodDisruptionBudgetdan foydalaning.
- Tashqi kirish: har bir broker uchun alohida manzil (LoadBalancer, NodePort yoki TLS passthrough bilan Ingress), aks holda yana `advertised.listeners` muammosi chiqadi.

## 17.6 Multi-DC va disaster recovery

| Sxema | Vosita | Xususiyatlari |
|---|---|---|
| **Active-passive** | MirrorMaker 2 | Boshqa DCdagi zaxira klaster, avariya paytida klientlarni o'tkazish |
| **Active-active** | Topic prefikslari bilan MirrorMaker 2 | Har bir DC lokal yozadi, ma'lumotlar ko'zgulanadi (`dc1.orders`) |
| **Stretched cluster** | 3 ta DCga yoyilgan bitta klaster | DClar orasida past kechikish kerak (< 10-20 ms), RPO = 0 |

**MirrorMaker 2** Kafka Connect asosida qurilgan, topiclarni, konfiguratsiyalarni, ACLlarni replikatsiya qiladi va consumer grouplarning offsetlarini translyatsiya qiladi (`emit.checkpoints.enabled`, `sync.group.offsets.enabled`). Yodda tuting: turli klasterlardagi offsetlar bir-biriga mos kelmaydi, iste'molchilarni o'tkazish offsetlarni translyatsiya qilishni talab qiladi.

## 17.7 Productiondagi Kafka antipatternlari

| Antipattern | Nimasi yomon | To'g'risi qanday |
|---|---|---|
| `replication.factor=1` | Disk buzilganda ma'lumot yo'qoladi | RF=3 |
| `auto.create.topics.enable=true` | Imlo xatosi standart sozlamali topic yaratadi | Topiclar IaC orqali (Terraform, Strimzi, GitOps) |
| Kafka so'rovlar uchun ma'lumotlar bazasi sifatida | Indekslar va maydonlar bo'yicha tanlash yo'q | Kafka log sifatida + ma'lumotlar bazasida materializatsiya |
| Har bir mijozga minglab topic | Metadata va partitionlar portlashi | Bitta topic + kalit/headerlar |
| 10-50 MB li xabarlar | Xotira va replikatsiyaga bosim | S3 orqali claim check |
| Har bir so'rovga yangi producer | Ulanishlar sizib chiqadi, batching yo'q | Har bir ilovaga bitta producer |
| `send()` xatolarini e'tiborsiz qoldirish | Ma'lumotlarning jimgina yo'qolishi | Callbackni qayta ishlash, xatolar metrikasi |
| Lag monitoringi yo'q | Muammo haqida foydalanuvchilardan bilasiz | Vaqt bilan ifodalangan lag alertlari |
| Poll-siklda uzoq sinxron qayta ishlash | Rebalance bo'roni | Kichikroq `max.poll.records`, partitionlarni pauza qilish |
| Butun topicga bitta partition orqali tartib | Masshtablash yo'q | Kalit bo'yicha tartib |

---

# Modul 18. Yakuniy loyiha: event-driven internet-do'kon

## 18.1 Arxitektura

```text
               HTTP
Client -----> Order Service ---(outbox + Debezium)---> shop.orders.events.v1
                                                          |
         +------------------------------+-----------------+-----------------+
         v                              v                                   v
  Payment Service                Inventory Service                 Analytics (Kafka Streams)
  (idempotent,                   (tovar rezervi)                   daqiqadagi buyurtmalar,
   Kafka tranzaksiyalari)               |                           kategoriyalar bo'yicha tushum
         |                              v                                   |
         v                     shop.inventory.events.v1                     v
 payments.transactions.events.v1                                   analytics.orders.stats.v1
         |                                                                  |
         v                                                                  v
 Notification Service (share group, email-workerlar)              ClickHouse (sink connector)
         |
         v
 notifications.email.dlq
```

## 18.2 Talablar

1. **Order Service** buyurtmani PostgreSQLga saqlaydi va hodisani bitta tranzaksiyada outboxga yozadi. Debezium `OrderCreated` ni e'lon qiladi.
2. **Topiclar**: RF=3, `min.insync.replicas=2`, kalit `order_id`, Schema Registryda `BACKWARD` rejimidagi Avro yoki Protobuf sxemalari.
3. **Payment Service** `OrderCreated` ni o'qiydi, to'lovni `event_id` bo'yicha idempotent tarzda yechadi, `PaymentSucceeded` yoki `PaymentFailed` ni e'lon qiladi.
4. **Inventory Service** tovarni rezerv qiladi; `PaymentFailed` bo'lganda rezervni bekor qiladi (xoreografiya orqali saga).
5. **Notification Service** share group orqali ishlaydi, SMTPning vaqtinchalik xatolarini retry qiladi, doimiylarini DLQga yuboradi.
6. Kafka Streamsdagi **Analytics** daqiqadagi buyurtmalarni va tushumni `exactly_once_v2` bilan hisoblaydi.
7. **Monitoring**: Prometheus + Grafana, URP, offline partitions va lag bo'yicha alertlar.
8. **Xavfsizlik**: SASL/SCRAM, har bir servisga alohida foydalanuvchi va ACL.

## 18.3 Xaos-test

- [ ] Yuklama paytida partition leaderini to'xtating: yo'qotish yo'q, klientlarda bir necha soniyadan uzoq xatolar yo'q.
- [ ] Payment Serviceni batch o'rtasida o'ldiring: qayta ishga tushgandan keyin ikki marta pul yechish yo'q.
- [ ] Buzuq xabar yuboring: u DLQda, qolganlari qayta ishlanmoqda.
- [ ] Debeziumni 10 daqiqaga to'xtating: ishga tushgandan keyin barcha hodisalar yetkazilgan.
- [ ] Yuklama ostida barcha brokerlarni rolling restart qiling.
- [ ] Sxemaga mos kelmaydigan o'zgarish qo'shing: Schema Registry uni rad etadi.

---

# Kafka CLI shpargalkasi

Quyidagi buyruqlar konteyner ichida (`docker exec -it kafka-1 bash`, keyin `cd /opt/kafka/bin`) yoki lokal o'rnatilgan Kafka distributivi bilan bajariladi.

```bash
BS=kafka-1:9092   # bootstrap server

# ---------- Topiclar ----------
kafka-topics.sh --bootstrap-server $BS --list
kafka-topics.sh --bootstrap-server $BS --create --topic t --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --describe --topic t
kafka-topics.sh --bootstrap-server $BS --alter --topic t --partitions 12
kafka-topics.sh --bootstrap-server $BS --delete --topic t
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions

# ---------- Konfiguratsiyalar ----------
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name t
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name t --add-config retention.ms=86400000
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name t --delete-config retention.ms
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-name 1 --all

# ---------- Producer / Consumer ----------
kafka-console-producer.sh --bootstrap-server $BS --topic t --property parse.key=true --property key.separator=:
kafka-console-consumer.sh --bootstrap-server $BS --topic t --from-beginning --property print.key=true --property print.offset=true
kafka-console-consumer.sh --bootstrap-server $BS --topic t --partition 0 --offset 100 --max-messages 10

# ---------- Consumer groups ----------
kafka-consumer-groups.sh --bootstrap-server $BS --list
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g --members --verbose
kafka-consumer-groups.sh --bootstrap-server $BS --group g --topic t --reset-offsets --to-earliest --execute
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group g

# ---------- Offsets ----------
kafka-get-offsets.sh --bootstrap-server $BS --topic t              # oxirgi offsetlar
kafka-get-offsets.sh --bootstrap-server $BS --topic t --time -2    # eng dastlabkilari

# ---------- Klaster / KRaft ----------
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication
kafka-features.sh --bootstrap-server $BS describe
kafka-broker-api-versions.sh --bootstrap-server $BS
kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list t
kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions

# ---------- Debug ----------
kafka-dump-log.sh --files /path/00000000000000000000.log --print-data-log
kafka-producer-perf-test.sh --topic t --num-records 1000000 --record-size 1024 --throughput -1 --producer-props bootstrap.servers=$BS
kafka-consumer-perf-test.sh --bootstrap-server $BS --topic t --messages 1000000
```

---

# Muhim sozlamalar shpargalkasi

## Producer

| Parametr | Standart qiymat | Tavsiya |
|---|---|---|
| `acks` | `all` | `all` |
| `enable.idempotence` | `true` | `true` |
| `linger.ms` | `5` | 5-50 |
| `batch.size` | `16384` | 32-256 KB |
| `compression.type` | `none` | `lz4` yoki `zstd` |
| `delivery.timeout.ms` | `120000` | SLAga qarab |
| `max.in.flight.requests.per.connection` | `5` | Idempotentlik bilan ≤ 5 |

## Consumer

| Parametr | Standart qiymat | Tavsiya |
|---|---|---|
| `group.protocol` | `classic` | 4.x dagi yangi ilovalar uchun `consumer` |
| `enable.auto.commit` | `true` | `false` + qayta ishlashdan keyin qo'lda commit |
| `auto.offset.reset` | `latest` | Ssenariyga qarab ongli tanlov |
| `max.poll.records` | `500` | Qayta ishlash vaqtiga qarab |
| `max.poll.interval.ms` | `300000` | Batchni qayta ishlashning maksimal vaqtidan katta |
| `isolation.level` | `read_uncommitted` | Tranzaksiyalarda `read_committed` |
| `group.instance.id` | yo'q | Kubernetesda static membership uchun belgilash |

## Topic va broker

| Parametr | Standart qiymat | Tavsiya |
|---|---|---|
| `replication.factor` / `default.replication.factor` | `1` | `3` |
| `min.insync.replicas` | `1` | `2` |
| `unclean.leader.election.enable` | `false` | Muhim ma'lumotlar uchun `false` |
| `retention.ms` | 7 kun | Talablarga qarab |
| `cleanup.policy` | `delete` | Holat uchun `compact` |
| `auto.create.topics.enable` | `true` | `false` |
| `compression.type` (topic) | `producer` | `producer` |

---

# Kafka bo'yicha ish suhbati savollari va javoblari

## Junior

<details>
<summary><b>1. Apache Kafka nima?</b></summary>

Hodisalarni oqim tarzida uzatuvchi taqsimlangan platforma. Hodisalarni partitionlarga bo'lingan, replikatsiya qilinadigan append-only logda saqlaydi va ko'plab mustaqil iste'molchilarga ularni istalgan pozitsiyadan o'qish imkonini beradi.
</details>

<details>
<summary><b>2. Kafka RabbitMQdan nimasi bilan farq qiladi?</b></summary>

Kafka xabarlarni o'qilgandan keyin ham saqlaydi va ularni offset bo'yicha beradi, partitionlar orqali masshtablanadi, event streaming va replay uchun mos keladi. RabbitMQ murakkab marshrutlashga ega navbatlar brokeri bo'lib, unda xabar odatda tasdiqlangandan keyin o'chiriladi.
</details>

<details>
<summary><b>3. Topic, partition va offset nima?</b></summary>

Topic hodisalarning nomlangan oqimi. Partition topic ichidagi tartiblangan jurnal va parallellik birligi. Offset partition ichidagi yozuvning tartib raqami.
</details>

<details>
<summary><b>4. Kafka xabarlar tartibini kafolatlaydimi?</b></summary>

Faqat bitta partition ichida. Bitta obyektning hodisalari tartib bilan kelishi uchun ular bir xil kalit bilan yuboriladi.
</details>

<details>
<summary><b>5. Consumer group nima?</b></summary>

Bitta `group.id` ga ega bo'lgan va topic partitionlarini bo'lib oladigan consumerlar to'plami: har bir partitionni guruhdagi bitta consumer o'qiydi. Turli guruhlar mustaqil o'qiydi.
</details>

<details>
<summary><b>6. Consumerlar partitionlardan ko'p bo'lsa, nima bo'ladi?</b></summary>

Ortiqcha consumerlar bo'sh turadi va partition bo'shashini kutadi. Tezlashish bo'lmaydi.
</details>

<details>
<summary><b>7. Kafka uchun ZooKeeper kerakmi?</b></summary>

Yo'q. Kafka 4.0 dan boshlab ZooKeeper olib tashlangan, metadata ichki o'rnatilgan KRaft protokoli bilan boshqariladi.
</details>

## Middle

<details>
<summary><b>8. Producer partitionni qanday tanlaydi?</b></summary>

Aniq ko'rsatilgan partition; aks holda `murmur2(key) % numPartitions`; kalit bo'lmasa, boshqasiga o'tishdan oldin bitta partition uchun batchni to'ldiradigan sticky partitioner.
</details>

<details>
<summary><b>9. acks=0, 1, all ni tushuntiring.</b></summary>

`0`: tasdiqsiz. `1`: leader tasdiqlaydi. `all`: ISRdagi barcha replikalar tasdiqlaydi. `all` ning ishonchliligi `min.insync.replicas` ga bog'liq.
</details>

<details>
<summary><b>10. ISR nima?</b></summary>

In-Sync Replicas: `replica.lag.time.max.ms` doirasida leaderga ulgurib borayotgan replikalar. Oddiy saylovda faqat ular leader bo'la oladi.
</details>

<details>
<summary><b>11. Nima uchun min.insync.replicas bo'lmasa acks=all yetarli emas?</b></summary>

ISR bitta leadergacha qisqarishi mumkin va `acks=all` amalda `acks=1` ga aylanadi. Leader yiqilsa, tasdiqlangan ma'lumotlar yo'qoladi. `min.insync.replicas=2` bunday vaziyatda yozishni taqiqlaydi.
</details>

<details>
<summary><b>12. Idempotent producer nima?</b></summary>

PID va har bir partition uchun sequence numberga ega producer: broker qayta yuborilgan batchlarni tashlab yuboradi. Bitta producer nusxasi ichidagi retrylarda dublikatlardan himoya qiladi.
</details>

<details>
<summary><b>13. At-least-once at-most-oncedan nimasi bilan farq qiladi?</b></summary>

At-most-once: qayta ishlashdan oldin commit, yo'qotish mumkin. At-least-once: qayta ishlashdan keyin commit, dublikatlar mumkin. Productionda odatda at-least-once va idempotent qayta ishlash qo'llanadi.
</details>

<details>
<summary><b>14. Rebalancega nima sabab bo'ladi va uni qanday kamaytirish mumkin?</b></summary>

Consumerning ulanishi va chiqib ketishi, heartbeatning o'tkazib yuborilishi, `max.poll.interval.ms` dan oshib ketish, obunaning o'zgarishi. Kamaytirish: static membership, cooperative rebalancing yoki yangi KIP-848 protokoli, tez qayta ishlash, to'g'ri `close()`.
</details>

<details>
<summary><b>15. Consumer lag nima va u qachon muammo?</b></summary>

High watermark va committed offset orasidagi farq. Lag barqaror o'sib borsa yoki qayta ishlash kechikishi SLAdan chiqib ketsa, muammo.
</details>

<details>
<summary><b>16. Log compaction retention deletedan nimasi bilan farq qiladi?</b></summary>

Delete eski segmentlarni vaqt yoki o'lcham bo'yicha o'chiradi. Compaction har bir kalitning oxirgi qiymatini saqlaydi; tombstone (value = null) kalitni o'chiradi.
</details>

<details>
<summary><b>17. Nima uchun partitionlarni ko'paytirish kalit bo'yicha tartibni buzadi?</b></summary>

`hash(key) % N` o'zgaradi va kalitning yangi hodisalari eskilaridan boshqa partitionga tushadi.
</details>

<details>
<summary><b>18. Har doim yiqiladigan xabarni qanday qayta ishlash kerak?</b></summary>

Cheksiz retry qilmaslik: cheklangan sondagi urinishlar, kechikishli retry-topiclar, keyin xato metadatasi va alert bilan DLQ.
</details>

## Senior

<details>
<summary><b>19. Kafka tranzaksiyalari qanday ishlaydi?</b></summary>

`transactional.id` + Transaction Coordinator + `__transaction_state`. Producer ma'lumotlarni bir nechta partitionga, offsetlarni esa `sendOffsetsToTransaction` orqali yozadi, commit control markerlarni yozadi. `read_committed` bilan ishlaydigan consumer Last Stable Offsetgacha o'qiydi. Producer epochi bo'yicha fencing zombilarni kesib tashlaydi.
</details>

<details>
<summary><b>20. Kafkada exactly-once qayerda tugaydi?</b></summary>

Kafka chegarasida. Tashqi qo'shimcha effektlar (ma'lumotlar bazasi, HTTP, email) idempotentlikni, outboxni yoki offsetni natija bilan bitta tranzaksiyada saqlashni talab qiladi.
</details>

<details>
<summary><b>21. High watermark va leader epoch nima?</b></summary>

HW ISRga replikatsiya qilingan va consumerlarga ko'rinadigan yozuvlar chegarasi. Leader epoch leader almashganidan keyin followerlarga logning farq qilib qolgan oxirgi qismini to'g'ri kesib tashlash imkonini beradi.
</details>

<details>
<summary><b>22. Nima uchun Kafka tez?</b></summary>

Ketma-ket I/O, page cache, zero-copy, batching va batch darajasidagi siqish, partitsiyalash, samarali binar protokol.
</details>

<details>
<summary><b>23. Partitionlar sonini qanday tanlash kerak?</b></summary>

O'sish uchun zaxira bilan `max(T/Tp, T/Tc)`, bunda tartibga oid cheklovlar, rebalance vaqti, brokerdagi replikalar soni va end-to-end kechikish hisobga olinadi.
</details>

<details>
<summary><b>24. Ma'lumotlar bazasiga yozgandan keyin hodisani qanday ishonchli e'lon qilish mumkin?</b></summary>

Transactional outbox: hodisa o'sha tranzaksiyaning o'zida outbox jadvaliga yoziladi, uni CDC (Debezium) yoki relay e'lon qiladi. Iste'molchilar idempotent.
</details>

<details>
<summary><b>25. Kafka uchun DRni qanday qurish kerak?</b></summary>

Offsetlarni translyatsiya qiladigan MirrorMaker 2 asosida active-passive yoki active-active, yoki past kechikishli 3 ta DCga yoyilgan stretched cluster. RPO/RTOni belgilash, o'tkazishni muntazam mashq qilish kerak.
</details>

<details>
<summary><b>26. Share Groups consumer groupsdan nimasi bilan farq qiladi?</b></summary>

Share group bir nechta consumerga bitta partitionni parallel o'qishga imkon beradi: har bir yozuv alohida tasdiqlanadi, qayta yetkazish va urinishlar limiti bor, lekin tartib kafolatlanmaydi.
</details>

<details>
<summary><b>27. Kalit tanlashda nima muhimroq: bir tekislikmi yoki tartibmi?</b></summary>

Biznes invariantiga bog'liq. Agar tartibning buzilishi to'g'rilikni buzsa (hisob balansi), tartib bir tekislikdan muhimroq. Qaynoq kalitlar alohida hal qilinadi.
</details>

<details>
<summary><b>28. Guruhlarning yangi KIP-848 protokoli qanday o'zgarishlar berdi?</b></summary>

Partitionlarni tayinlash brokerda hisoblanadi, rebalance inkremental va global sinxronlashsiz, sekin ishtirokchi guruhni bloklamaydi, timeoutlar va assignor server tomonida sozlanadi.
</details>

<details>
<summary><b>29. Yozish kechikishining p99 qiymati o'sishini qanday tekshirgan bo'lardingiz?</b></summary>

Fazalar bo'yicha `TotalTimeMs` metrikalari (RequestQueue, Local, Remote), `RequestHandlerAvgIdlePercent`, ISR shrinks, GC pauzalari, disk va tarmoq, klientlardagi trafik va batch o'lchamining o'zgarishi, «shovqinli» klientlar va kvotalar.
</details>

<details>
<summary><b>30. Kafkani qachon ishlatmaslik kerak?</b></summary>

Replaysiz kichik yuklama, sinxron javob kerak, murakkab marshrutlash va xabarlar prioriteti kerak, ekspluatatsiyaga resurs yo'q.
</details>

---

# FAQ: Apache Kafka haqida ko'p beriladigan savollar

**Kafkani noldan qanday o'rganish mumkin?**

Ushbu kursning 0-6 modullarini tartib bilan o'ting, Dockerda uchta brokerdan iborat klasterni ko'taring, o'z tilingizda producer va consumer yozing, keyin klasterni buzing va uning xatti-harakatini kuzating. Shundan so'ng tranzaksiyalar, Kafka Connect va Kafka Streamsga o'ting.

**Kafkani o'zlashtirish uchun qancha vaqt kerak?**

Asosiy tushuncha va birinchi ishlaydigan servislar: 1-2 hafta. Replikatsiya, yetkazish kafolatlari va monitoring bilan ishonchli production darajasi: 1-3 oy amaliyot.

**Kafka uchun qaysi dasturlash tilini tanlash kerak?**

Etalon klient Javada yozilgan, Kafka Streams va Kafka Connect JVMda ishlaydi. Go uchun franz-go va confluent-kafka-go, Python uchun confluent-kafka, .NET uchun Confluent.Kafka, Node.js uchun KafkaJS va confluent-kafka-javascript bor.

**Kafka xabar brokerimi yoki ma'lumotlar bazasimi?**

Bu taqsimlangan hodisalar logi. U xabar brokeri sifatida ham, hodisalarning uzoq muddatli ombori sifatida ham ishlatiladi, lekin indekslari va ixtiyoriy so'rovlari bor ma'lumotlar bazasining o'rnini bosmaydi.

**2026-yilda ZooKeeper kerakmi?**

Yo'q. Kafka 4.x faqat KRaft rejimida ishlaydi.

**Kafka soniyasiga nechta xabarga bardosh beradi?**

Tipik apparatdagi klaster soniyasiga yuz minglab va millionlab xabarni qayta ishlaydi. Haqiqiy chegara xabarlar o'lchamiga, `acks` ga, siqishga, disklarga, tarmoqqa va partitionlar soniga bog'liq.

**Kafka xabarlarni yo'qotishi mumkinmi?**

Noto'g'ri sozlanganda mumkin: RF=1, `acks=1`, `min.insync.replicas=1`, yoqilgan unclean leader election, `send()` xatolarini e'tiborsiz qoldirish, offsetni qayta ishlashdan oldin commit qilish. 4 va 5-modullardagi sozlamalar bilan bitta broker ishdan chiqqanda tasdiqlangan ma'lumotlarning yo'qolishi istisno qilinadi.

**Kafkada kechiktirilgan xabarlar yoki kechikishni qanday qilish mumkin?**

Ichki o'rnatilgan kechiktirilgan xabarlar yo'q. Partitionlarni pauza qiladigan retry-topiclar, tashqi rejalashtiruvchi (scheduler) yoki vazifalarni ma'lumotlar bazasida saqlab, vaqti kelganda e'lon qilish qo'llanadi.

**Kafka yoki RabbitMQ: qaysi birini tanlash kerak?**

Kafka event streaming, analitika, CDC, yuqori yuklama va replay uchun. RabbitMQ vazifalar navbatlari, RPC va murakkab marshrutlash uchun. Share Groups paydo bo'lishi bilan Kafka navbat ssenariylarining bir qismini qoplaydi.

**Qaysi biri yaxshi: o'z Kafkangizmi yoki managed-servismi?**

Managed-servis ekspluatatsiya, yangilanishlar va monitoringga ketadigan vaqtni tejaydi. O'z Kafkangiz narx va konfiguratsiya ustidan nazorat beradi, lekin jamoadan ekspertiza talab qiladi.

---

# Kafka lug'ati

| Termin | Ta'rif |
|---|---|
| **Acks** | Producer yozuvini tasdiqlash darajasi |
| **Broker** | Partitionlarni saqlaydigan Kafka serveri |
| **Bootstrap servers** | Metadatani olish uchun brokerlarning boshlang'ich manzillari |
| **CDC** | Change Data Capture, ma'lumotlar bazasidagi o'zgarishlarni ushlab olish |
| **Changelog topic** | Kafka Streams state storeni saqlaydigan topic |
| **Cleanup policy** | Tozalash siyosati: `delete` yoki `compact` |
| **Consumer group** | Partitionlarni bo'lib oladigan consumerlar guruhi |
| **Consumer lag** | Guruhning log oxiridan ortda qolishi |
| **Controller** | Klaster metadatasini boshqaradigan tugun |
| **DLQ** | Dead Letter Queue, qayta ishlab bo'lmaydigan xabarlar uchun topic |
| **Exactly-once (EOS)** | Aynan bir marta qayta ishlash semantikasi |
| **Fencing** | Producer yoki consumerning eskirgan nusxasini kesib tashlash |
| **High watermark** | ISRga replikatsiya qilingan oxirgi offset |
| **Idempotent producer** | Retrylarda dublikatlarni istisno qiladigan producer |
| **ISR** | In-Sync Replicas, sinxron replikalar |
| **KRaft** | Raft asosidagi Kafka metadata konsensus protokoli |
| **Leader** | Yozishni qabul qiladigan partition replikasi |
| **Log compaction** | Kalit bo'yicha oxirgi qiymatni saqlash |
| **LEO** | Log End Offset, yozish uchun keyingi offset |
| **Offset** | Partitiondagi yozuv raqami |
| **Outbox** | Hodisalarni ma'lumotlar bazasi jadvali orqali ishonchli e'lon qilish patterni |
| **Partition** | Topic ichidagi tartiblangan jurnal |
| **Rebalance** | Guruhdagi partitionlarni qayta taqsimlash |
| **Replication factor** | Partition nusxalari soni |
| **Retention** | Ma'lumotlarni saqlash muddati yoki hajmi |
| **Schema Registry** | Xabar sxemalarini saqlash va tekshirish servisi |
| **Segment** | Partition logining diskdagi fayli |
| **Share group** | Navbat semantikasiga ega va har bir yozuvni alohida tasdiqlaydigan guruh |
| **Tombstone** | Compacted-topicda kalitni o'chiradigan, value = null bo'lgan yozuv |
| **Topic** | Hodisalarning nomlangan oqimi |
| **Transactional ID** | Tranzaksion producerning identifikatori |

---

# Rasmiy manbalar va keyin nima o'qish kerak

- [Apache Kafka hujjatlari](https://kafka.apache.org/documentation/)
- [Apache Kafka relizlari va e'lonlari](https://kafka.apache.org/blog/)
- [Kafka Improvement Proposals (KIP)](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)
- [KIP-848: consumer groupning yangi protokoli](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [Debezium hujjatlari](https://debezium.io/documentation/)
- [Strimzi: Kubernetesda Kafka](https://strimzi.io/documentation/)
- Kitoblar: «Kafka: The Definitive Guide» (2-nashr), «Designing Data-Intensive Applications» (Martin Kleppmann), «Kafka Streams in Action».

---

## Kursga qanday yordam berish mumkin

- ⭐ Kursni ko'proq dasturchilar ko'rishi uchun repozitoriyga yulduzcha qo'ying.
- Xato yoki noaniqlik topdingizmi? Issue yoki Pull Request yarating.
- Kursni jamoangiz va Kafkani o'rganayotgan hamkasblaringiz bilan baham ko'ring.

**Kursning asosiy mavzulari:** Apache Kafka kursi, Kafka noldan, Kafka bepul kurs, Kafka o'zbek tilida, Kafka tutorial, KRaft, Kafka Docker Compose, Kafka producer consumer, consumer group, rebalance, exactly-once, Kafka transactions, transactional outbox, Kafka Streams, Kafka Connect, Debezium CDC, Schema Registry, Avro, Protobuf, Kafka monitoring, Kafka xavfsizligi, Kubernetesda Kafka, Strimzi, Kafka vs RabbitMQ, Kafka bo'yicha ish suhbati savollari.
