# 📊 Rancang Bangun Data Warehouse pada Sistem Informasi Akademik Perguruan Tinggi

## 1. Pendahuluan

Proyek ini merupakan implementasi Data Warehouse sederhana untuk sistem informasi akademik perguruan tinggi. Tujuannya adalah untuk mengintegrasikan data dari berbagai sumber seperti mahasiswa, mata kuliah, dosen, nilai, dan keuangan ke dalam satu repositori terpusat. Hal ini mendukung:

- Analisis kinerja akademik
- Pemantauan kelulusan
- Pengambilan keputusan berbasis data

Proses ETL (Extract, Transform, Load) diimplementasikan menggunakan **Python** dan **Pandas**, sedangkan **MySQL** digunakan sebagai sistem manajemen basis data. Visualisasi analitik disajikan menggunakan **Matplotlib** dan **Plotly Express**.

---

## 2. Arsitektur Proyek

Proyek ini mengikuti alur ETL konvensional:

1. **Extract**: Data diambil dari sumber simulasi (menggunakan Pandas DataFrame).
2. **Transform**: Data dibersihkan, distandarisasi, dan diubah menjadi bentuk dimensi dan fakta sesuai model skema bintang.
3. **Load**: Data dimasukkan ke MySQL sebagai Data Warehouse.
4. **Visualize**: Hasil analisis divisualisasikan dengan grafik.


---

## 3. Struktur Data Warehouse

Data Warehouse dirancang menggunakan **Star Schema** dengan:

### 📁 Tabel Fakta

- **fakta_nilai**: Mencatat nilai akhir mahasiswa per mata kuliah (relasi ke mahasiswa, dosen, mata kuliah, dan semester).
- **fakta_ipk**: Ringkasan IPK setiap mahasiswa.

### 📂 Tabel Dimensi

- **dim_mahasiswa**: Informasi mahasiswa (NIM, nama, fakultas, prodi, tahun masuk, status beasiswa).
- **dim_matakuliah**: Detail mata kuliah (kode, nama, SKS, semester).
- **dim_dosen**: Informasi dosen (NIDN, nama, jabatan, fakultas).
- **dim_semester**: Data semester (tahun ajaran, semester).


---

## 4. Persyaratan Sistem

Pastikan sistem Anda memiliki:

- Python ≥ 3.9
- Docker Desktop
- MySQL (dijalankan melalui Docker Compose)

**Konfigurasi Docker:**

- Service: `akademik-db` (container: `akademik-mysql`)
- Port: `13306` di host → `3306` di container

**Kredensial:**

- `root:rootpassword`
- atau `akademik_user:akademik123`

---

## 5. Instalasi & Konfigurasi

### 🔧 Kloning Repositori

```bash
git clone https://github.com/anraaa/dw
cd dw
```

### 📦 Buat & Aktifkan Virtual Environment

**Windows:**
```bash
python -m venv venv
.\venv\Scripts\activate
```

**macOS/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

### 📥 Instal Dependensi

```bash
pip install -r requirements.txt
```

**Isi `requirements.txt`:**
```
pandas==2.0.3
mysql-connector-python==8.1.0
matplotlib==3.7.2
plotly==5.15.0
```

### 🐳 Jalankan MySQL via Docker

```bash
docker compose up -d
```

Verifikasi container:
```bash
docker compose ps
docker compose logs akademik-mysql
```

Jika ingin masuk ke bash mysql
```bash
docker exec -it akademik-mysql -u root -p
```

**(Opsional) Ubah Kredensial Koneksi MySQL:**

```python
connection = mysql.connector.connect(
    host='localhost',
    port=13306,
    user='akademik_user',
    password='akademik123',
    database='dw_akademik',
    connect_timeout=3000
)
```

---

## 6. Menjalankan Proyek

Jalankan proses ETL:

```bash
python3 etl-script.py
```

- Output log akan menunjukkan progres ETL.
- Jendela Matplotlib akan muncul.
- Plot interaktif Plotly akan terbuka di browser.

---

## 7. Output & Visualisasi

### 📊 Visualisasi Utama:

- **Rata-rata IPK per Fakultas** (Matplotlib): Horizontal bar chart untuk membandingkan IPK rata-rata.
- **Persentase Penerima Beasiswa per Fakultas** (Matplotlib): Bar chart untuk melihat distribusi beasiswa.
- **Distribusi IPK Mahasiswa per Prodi** (Plotly): Box plot interaktif untuk menganalisis distribusi IPK.

---

## 8. Pengujian & Validasi

✔️ Pengujian mencakup:

- Validasi baris antara DataFrame dan MySQL.
- Integritas data (null check & format).
- Akurasi perhitungan (terutama IPK & agregat).


---

## 9. Pengembangan Selanjutnya

🛠 Beberapa ide pengembangan:

- Implementasi Incremental Load
- Integrasi data keuangan lebih detail
- Menambah dimensi waktu granular (tanggal, kuartal)
- Integrasi dengan Power BI / Tableau
- Penanganan Slowly Changing Dimension (SCD)
- Skrip untuk shutdown otomatis container Docker

---

## 10. Lisensi

Proyek ini dilisensikan di bawah MIT License.