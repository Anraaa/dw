# Sistem Data Warehouse Akademik
Sistem Data Warehouse Akademik adalah proyek yang dirancang untuk mengintegrasikan dan menganalisis data akademik dari berbagai sumber di lingkungan perguruan tinggi. Proyek ini bertujuan untuk menyediakan platform data terpusat yang mendukung pengambilan keputusan berbasis data untuk manajemen.

# Pendahuluan
## A. Latar Belakang
Sistem Informasi Akademik di perguruan tinggi menghasilkan data yang tersebar di berbagai sumber, seperti sistem KRS (Kartu Rencana Studi), nilai mahasiswa, kehadiran, pembayaran, dan data dosen. Integrasi data ini diperlukan untuk mendukung analisis kinerja akademik, pemantauan kelulusan, evaluasi kurikulum, dan pelaporan kepada stakeholders. Data warehouse dibangun untuk menyediakan data terintegrasi yang mendukung pengambilan keputusan berbasis data.

## B. Tujuan Proyek
- Membangun sistem data warehouse untuk analisis data akademik.
- Memudahkan pelaporan kinerja akademik (IPK, tingkat kelulusan, evaluasi dosen).
- Mendukung pengambilan keputusan manajemen perguruan tinggi.

## C. Ruang Lingkup
Data yang diproses meliputi:
- Data Mahasiswa: NIM, nama, fakultas, program studi.
- Data Mata Kuliah: Kode MK, nama MK, SKS, semester.
- Data Nilai: Nilai per mata kuliah, IPK, status kelulusan.
- Data Dosen: NIDN, nama, jabatan.
- Data Beasiswa: Status beasiswa mahasiswa.

# Batasan Proyek:
- Fokus pada data historis (saat ini disimulasikan).
- Tidak mencakup data penelitian dan pengabdian masyarakat.
- Data keuangan hanya diekstrak dan status_beasiswa digunakan dalam transformasi, belum sepenuhnya diintegrasikan ke dalam fakta atau dimensi keuangan terpisah.
- Proses ETL saat ini adalah beban penuh (full load), tidak inkremental.
  
---

🔧 Teknologi yang Digunakan
Python (Untuk proses ETL)

Pandas (Transformasi dan manipulasi data)

PostgreSQL / MariaDB (Database untuk Data Warehouse)

Docker & Docker Compose (Containerization)

📜 Fitur Utama
Integrasi Data Akademik: Menggabungkan data dari berbagai sumber (mahasiswa, nilai, dosen, mata kuliah) menjadi satu.

Skema Bintang (Star Schema): Dirancang menggunakan model dimensional untuk analisis multidimensi yang efisien.

Proses ETL Terstruktur: Proses Extract, Transform, Load yang jelas untuk memuat data dari sumber ke data warehouse.

Dasbor Analitik: Data yang dihasilkan dapat dihubungkan ke tools BI untuk membuat laporan visual interaktif.

📖 Metodologi Perancangan
Proyek ini dirancang menggunakan metodologi Kimball 9-Step untuk Dimensional Modeling. Tahapan utamanya meliputi:

Pemilihan Proses Bisnis: Fokus pada proses analisis kinerja akademik.

Penentuan Grain: Menetapkan tingkat detail data, yaitu nilai per mahasiswa per mata kuliah per semester.

(Tahapan selanjutnya mengikuti metodologi Kimball...)

---

📦 Instalasi
Jika ingin menjalankan proyek ini secara lokal, ikuti langkah-langkah berikut.

📋 Prasyarat
Git

Docker & Docker Compose

1. Clone Repository & Siapkan Konfigurasi
```
# Clone repository
git clone <repo_url>

# Masuk ke direktori utama dan direktori aplikasi
cd infrareport/sampleapp

# Salin file environment untuk konfigurasi
cp .env.example .env
```

Setelah itu, buka file .env dan sesuaikan konfigurasi database (DB_DATABASE, DB_USERNAME, DB_PASSWORD) agar cocok dengan environment di file docker-compose.yml Anda.

2. Build & Jalankan Container Docker
Perintah ini akan membangun image dan menyalakan semua layanan (Nginx, PHP, MariaDB) di latar belakang.

```
docker compose up -d --build
```

3. Jalankan Perintah Setup di Dalam Container
Semua perintah composer dan artisan harus dijalankan di dalam container PHP (sample).

```
# Install dependensi PHP
docker exec -it sample bash
composer install

# Generate kunci aplikasi Laravel
php artisan key:generate

# Buat link dari storage ke folder public
php artisan storage:link

# Jalankan migrasi dan seeding database
php artisan migrate --seed
```

4. Atur Izin Akses Folder
Langkah ini krusial untuk menghindari error 500. Perintah ini memberikan izin kepada server untuk menulis file log dan cache.

```
chown -R www-data:www-data storage/*
chwon -R www-data:www-data bootstrap/*
```

5. Inisialisasi Proyek
```
php artisan project:init
```

6. Jalankan Proses ETL
```
php artisan etl:academic
```

5️⃣ Selesai!

📌 Catatan
Sumber Data: Data sumber saat ini disimulasikan dalam bentuk hard code pada proses ETL.

Tujuan: Proyek ini fokus pada pembelajaran konsep dan penerapan data warehouse, bukan untuk penggunaan produksi.

🤝 Kontribusi
Jika ingin berkontribusi dalam proyek ini, silakan fork repository dan kirimkan pull request.

💡 Dibangun dengan pemahaman mendalam tentang Dimensional Modeling dan proses ETL.*