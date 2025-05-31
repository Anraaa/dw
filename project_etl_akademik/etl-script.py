import pandas as pd
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime

# =============================================
# 1. EXTRACT (Mengambil Data dari Berbagai Sumber)
# =============================================
def extract_data():
    """Fungsi untuk mengekstrak data dari berbagai sumber sistem akademik"""
    print("\n[1] Extracting data from multiple sources...")
    
    # 1. Data Mahasiswa (dari database akademik)
    data_mahasiswa = {
        'nim': ['10120201', '10120202', '10120203', '10220201', '10320201'],
        'nama': ['Andi Wijaya', 'Budi Santoso', 'Citra Lestari', 'Dian Pratama', 'Eka Surya'],
        'fakultas': ['Teknik', 'Teknik', 'Ekonomi', 'Sains', 'Ekonomi'],
        'prodi': ['Informatika', 'Informatika', 'Manajemen', 'Fisika', 'Akuntansi'],
        'tahun_masuk': [2020, 2020, 2021, 2020, 2021],
        'status_beasiswa': ['Ya', 'Tidak', 'Ya', 'Tidak', 'Ya']
    }
    df_mahasiswa = pd.DataFrame(data_mahasiswa)
    
    # 2. Data Mata Kuliah (dari sistem kurikulum)
    data_matakuliah = {
        'kode_mk': ['INF101', 'INF102', 'MAN201', 'FIS101', 'AKU301'],
        'nama_mk': ['Basis Data', 'Algoritma', 'Manajemen Keuangan', 'Fisika Dasar', 'Akuntansi Lanjut'],
        'sks': [3, 4, 3, 4, 3],
        'semester_mk': [3, 2, 4, 1, 5]  # semester dalam kurikulum
    }
    df_matakuliah = pd.DataFrame(data_matakuliah)
    
    # 3. Data Dosen (dari sistem SDM)
    data_dosen = {
        'nidn': ['001203', '001204', '002101', '003102', '003201'],
        'nama_dosen': ['Dr. Ahmad Sanusi', 'Prof. Bambang Setiawan', 'Dr. Citra Dewi', 'Dr. Dodi Pratomo', 'Dr. Erni Wulandari'],
        'jabatan': ['Lektor', 'Guru Besar', 'Lektor', 'Asisten Ahli', 'Lektor'],
        'fakultas': ['Teknik', 'Teknik', 'Ekonomi', 'Sains', 'Ekonomi']
    }
    df_dosen = pd.DataFrame(data_dosen)
    
    # 4. Data Nilai (dari sistem KRS dan nilai)
    data_nilai = {
        'nim': ['10120201', '10120201', '10120202', '10120203', '10220201', '10320201'],
        'kode_mk': ['INF101', 'INF102', 'INF101', 'MAN201', 'FIS101', 'AKU301'],
        'nidn_dosen': ['001203', '001204', '001203', '002101', '003102', '003201'],
        'tahun_ajaran': ['2021/2022', '2021/2022', '2021/2022', '2022/2023', '2022/2023', '2022/2023'],
        'semester': ['Ganjil', 'Genap', 'Ganjil', 'Ganjil', 'Genap', 'Ganjil'],
        'nilai_akhir': [85, 78, 90, 82, 75, 88],
        'status_kelulusan': ['Lulus', 'Lulus', 'Lulus', 'Lulus', 'Lulus', 'Lulus']
    }
    df_nilai = pd.DataFrame(data_nilai)
    
    # 5. Data Keuangan (dari sistem keuangan)
    data_keuangan = {
        'nim': ['10120201', '10120202', '10120203', '10220201', '10320201'],
        'tahun_ajaran': ['2022/2023', '2022/2023', '2022/2023', '2022/2023', '2022/2023'],
        'semester_keuangan': ['Ganjil', 'Ganjil', 'Ganjil', 'Ganjil', 'Ganjil'],
        'total_pembayaran': [7500000, 7500000, 6500000, 7000000, 6500000],
        'status_pembayaran': ['Lunas', 'Lunas', 'Lunas', 'Cicilan', 'Lunas']
    }
    df_keuangan = pd.DataFrame(data_keuangan)
    
    print("Data extraction completed successfully!")
    return df_mahasiswa, df_matakuliah, df_dosen, df_nilai, df_keuangan

# =============================================
# 2. TRANSFORM (Pembersihan & Transformasi Data)
# =============================================
def transform_data(df_mahasiswa, df_matakuliah, df_dosen, df_nilai, df_keuangan):
    """Fungsi untuk membersihkan dan mentransformasi data"""
    print("\n[2] Transforming data...")
    
    # 1. Standarisasi format data
    df_mahasiswa['nim'] = df_mahasiswa['nim'].str.upper().str.strip()
    df_nilai['nim'] = df_nilai['nim'].str.upper().str.strip()
    df_keuangan['nim'] = df_keuangan['nim'].str.upper().str.strip()
    
    df_matakuliah['kode_mk'] = df_matakuliah['kode_mk'].str.upper().str.strip()
    df_nilai['kode_mk'] = df_nilai['kode_mk'].str.upper().str.strip()
    
    df_dosen['nidn'] = df_dosen['nidn'].str.strip()
    df_nilai['nidn_dosen'] = df_nilai['nidn_dosen'].str.strip()
    
    # 2. Membuat ID unik untuk dimensi
    df_mahasiswa['id_mahasiswa'] = range(1, len(df_mahasiswa) + 1)
    df_matakuliah['id_matakuliah'] = range(1, len(df_matakuliah) + 1)
    df_dosen['id_dosen'] = range(1, len(df_dosen) + 1)
    
    # 3. Membuat tabel dimensi semester
    unique_semesters = df_nilai[['tahun_ajaran', 'semester']].drop_duplicates()
    unique_semesters['id_semester'] = range(1, len(unique_semesters) + 1)
    df_semester = unique_semesters
    
    # 4. Menggabungkan data untuk fakta nilai
    df_fakta_nilai = pd.merge(df_nilai, df_mahasiswa, on='nim', how='left')
    df_fakta_nilai = pd.merge(df_fakta_nilai, df_matakuliah, on='kode_mk', how='left')
    df_fakta_nilai = pd.merge(df_fakta_nilai, df_dosen, left_on='nidn_dosen', right_on='nidn', how='left')
    df_fakta_nilai = pd.merge(df_fakta_nilai, df_semester, on=['tahun_ajaran', 'semester'], how='left')
    
    # Memilih kolom yang diperlukan untuk fakta nilai
    df_fakta_nilai = df_fakta_nilai[[
        'id_mahasiswa', 'id_matakuliah', 'id_dosen', 'id_semester',
        'nilai_akhir', 'status_kelulusan'
    ]]
    
    # 5. Menghitung IPK per mahasiswa
    df_ipk = pd.merge(df_nilai, df_matakuliah, on='kode_mk', how='left')
    df_ipk['nilai_setara'] = df_ipk['nilai_akhir'].apply(
        lambda x: 4 if x >= 80 else (3 if x >= 70 else (2 if x >= 60 else 1)))
    df_ipk['total_point'] = df_ipk['nilai_setara'] * df_ipk['sks']
    
    df_total_sks = df_ipk.groupby('nim')['sks'].sum().reset_index()
    df_total_point = df_ipk.groupby('nim')['total_point'].sum().reset_index()
    
    df_ipk_final = pd.merge(df_total_point, df_total_sks, on='nim')
    df_ipk_final['ipk'] = df_ipk_final['total_point'] / df_ipk_final['sks']
    df_ipk_final = pd.merge(df_ipk_final, df_mahasiswa, on='nim', how='left')
    
    # 6. Menghitung statistik fakultas
    df_statistik_fakultas = df_ipk_final.groupby('fakultas').agg(
        jumlah_mahasiswa=('nim', 'count'),
        rata_ipk=('ipk', 'mean'),
        persen_beasiswa=('status_beasiswa', lambda x: (x == 'Ya').mean() * 100)
    ).reset_index()
    
    print("Data transformation completed successfully!")
    return df_mahasiswa, df_matakuliah, df_dosen, df_semester, df_fakta_nilai, df_ipk_final, df_statistik_fakultas

# =============================================
# 3. LOAD (Memuat Data ke Data Warehouse)
# =============================================
def create_database_connection():
    """Membuat koneksi ke database MySQL"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=13306,
            user='root',
            password='rootpassword',
            database='dw_akademik',
            connect_timeout=3000
        )
        print("Berhasil terhubung ke database!")
        return connection
    except Error as e:
        print(f"Gagal terhubung ke database: {e}")
        return None

def create_database_tables(connection):
    """Membuat struktur tabel data warehouse"""
    try:
        cursor = connection.cursor()
        
        # Create database if not exists
        cursor.execute("CREATE DATABASE IF NOT EXISTS dw_akademik")
        cursor.execute("USE dw_akademik")
        
        # Drop existing tables if they exist
        cursor.execute("DROP TABLE IF EXISTS fakta_nilai")
        cursor.execute("DROP TABLE IF EXISTS fakta_ipk")
        cursor.execute("DROP TABLE IF EXISTS dim_mahasiswa")
        cursor.execute("DROP TABLE IF EXISTS dim_matakuliah")
        cursor.execute("DROP TABLE IF EXISTS dim_dosen")
        cursor.execute("DROP TABLE IF EXISTS dim_semester")
        
        # Create dimension tables
        cursor.execute("""
        CREATE TABLE dim_mahasiswa (
            id_mahasiswa INT PRIMARY KEY,
            nim VARCHAR(20),
            nama VARCHAR(100),
            fakultas VARCHAR(50),
            prodi VARCHAR(50),
            tahun_masuk INT,
            status_beasiswa VARCHAR(10)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE dim_matakuliah (
            id_matakuliah INT PRIMARY KEY,
            kode_mk VARCHAR(20),
            nama_mk VARCHAR(100),
            sks INT,
            semester_mk INT
        )
        """)
        
        cursor.execute("""
        CREATE TABLE dim_dosen (
            id_dosen INT PRIMARY KEY,
            nidn VARCHAR(20),
            nama_dosen VARCHAR(100),
            jabatan VARCHAR(50),
            fakultas VARCHAR(50)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE dim_semester (
            id_semester INT PRIMARY KEY,
            tahun_ajaran VARCHAR(20),
            semester VARCHAR(10)
        )
        """)
        
        # Create fact tables
        cursor.execute("""
        CREATE TABLE fakta_nilai (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_mahasiswa INT,
            id_matakuliah INT,
            id_dosen INT,
            id_semester INT,
            nilai_akhir DECIMAL(5,2),
            status_kelulusan VARCHAR(20),
            FOREIGN KEY (id_mahasiswa) REFERENCES dim_mahasiswa(id_mahasiswa),
            FOREIGN KEY (id_matakuliah) REFERENCES dim_matakuliah(id_matakuliah),
            FOREIGN KEY (id_dosen) REFERENCES dim_dosen(id_dosen),
            FOREIGN KEY (id_semester) REFERENCES dim_semester(id_semester)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE fakta_ipk (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_mahasiswa INT,
            total_point DECIMAL(10,2),
            total_sks INT,
            ipk DECIMAL(3,2),
            FOREIGN KEY (id_mahasiswa) REFERENCES dim_mahasiswa(id_mahasiswa)
        )
        """)
        
        connection.commit()
        print("Berhasil membuat struktur tabel data warehouse!")
        
    except Error as e:
        print(f"Gagal membuat tabel: {e}")
    finally:
        if cursor:
            cursor.close()

def load_to_dw(df, table_name, connection):
    """Memuat data ke tabel data warehouse"""
    try:
        cursor = connection.cursor()
        
        # Disable foreign key checks temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Clear existing data - use DELETE instead of TRUNCATE for tables with FK constraints
        cursor.execute(f"DELETE FROM {table_name}")
        
        # Insert data based on table name
        if table_name == "dim_mahasiswa":
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO dim_mahasiswa (id_mahasiswa, nim, nama, fakultas, prodi, tahun_masuk, status_beasiswa)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (row['id_mahasiswa'], row['nim'], row['nama'], row['fakultas'], 
                      row['prodi'], row['tahun_masuk'], row['status_beasiswa']))
        
        elif table_name == "dim_matakuliah":
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO dim_matakuliah (id_matakuliah, kode_mk, nama_mk, sks, semester_mk)
                VALUES (%s, %s, %s, %s, %s)
                """, (row['id_matakuliah'], row['kode_mk'], row['nama_mk'], 
                      row['sks'], row['semester_mk']))
        
        elif table_name == "dim_dosen":
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO dim_dosen (id_dosen, nidn, nama_dosen, jabatan, fakultas)
                VALUES (%s, %s, %s, %s, %s)
                """, (row['id_dosen'], row['nidn'], row['nama_dosen'], 
                      row['jabatan'], row['fakultas']))
        
        elif table_name == "dim_semester":
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO dim_semester (id_semester, tahun_ajaran, semester)
                VALUES (%s, %s, %s)
                """, (row['id_semester'], row['tahun_ajaran'], row['semester']))
        
        elif table_name == "fakta_nilai":
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO fakta_nilai (id_mahasiswa, id_matakuliah, id_dosen, id_semester, nilai_akhir, status_kelulusan)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (row['id_mahasiswa'], row['id_matakuliah'], row['id_dosen'], 
                      row['id_semester'], row['nilai_akhir'], row['status_kelulusan']))
        
        elif table_name == "fakta_ipk":
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO fakta_ipk (id_mahasiswa, total_point, total_sks, ipk)
                VALUES (%s, %s, %s, %s)
                """, (row['id_mahasiswa'], row['total_point'], row['sks'], row['ipk']))
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        connection.commit()
        print(f"Berhasil memuat data ke tabel {table_name}!")
        
    except Error as e:
        print(f"Gagal memuat data ke {table_name}: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()
            
# =============================================
# 4. VISUALISASI (Analisis Data Akademik)
# =============================================
def visualize_data(df_ipk, df_statistik_fakultas):
    """Fungsi untuk membuat visualisasi data"""
    print("\n[4] Generating data visualizations...")
    
    # 1. Visualisasi IPK per Fakultas
    plt.figure(figsize=(10, 6))
    df_statistik_fakultas.sort_values('rata_ipk', ascending=True).plot(
        kind='barh', x='fakultas', y='rata_ipk', 
        color=['skyblue', 'lightgreen', 'salmon'], legend=False)
    plt.title('Rata-Rata IPK per Fakultas', pad=20)
    plt.xlabel('IPK')
    plt.ylabel('Fakultas')
    plt.xlim(0, 4)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
    
    # 2. Persentase Penerima Beasiswa per Fakultas
    plt.figure(figsize=(10, 6))
    df_statistik_fakultas.sort_values('persen_beasiswa', ascending=True).plot(
        kind='barh', x='fakultas', y='persen_beasiswa',
        color=['gold', 'lightcoral', 'lightblue'])
    plt.title('Persentase Penerima Beasiswa per Fakultas', pad=20)
    plt.xlabel('Persentase (%)')
    plt.ylabel('Fakultas')
    plt.xlim(0, 100)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
    
    # 3. Interaktif: Distribusi IPK Mahasiswa
    fig = px.box(df_ipk, x='prodi', y='ipk', color='fakultas',
                 title='Distribusi IPK Mahasiswa per Program Studi',
                 hover_data=['nama', 'tahun_masuk'])
    fig.update_layout(xaxis_title='Program Studi', yaxis_title='IPK')
    fig.show()

# =============================================
# PROGRAM UTAMA
# =============================================
if __name__ == "__main__":
    print("\n=== SISTEM ETL DATA WAREHOUSE AKADEMIK ===")
    print("Memulai proses ETL...")
    
    connection = None
    try:
        # 1. Extract data dari berbagai sumber
        df_mahasiswa, df_matakuliah, df_dosen, df_nilai, df_keuangan = extract_data()
        
        # 2. Transform data untuk data warehouse
        (df_mahasiswa, df_matakuliah, df_dosen, df_semester, 
         df_fakta_nilai, df_ipk, df_statistik_fakultas) = transform_data(
             df_mahasiswa, df_matakuliah, df_dosen, df_nilai, df_keuangan)
        
        # 3. Load data ke data warehouse
        connection = create_database_connection()
        if connection and connection.is_connected():
            create_database_tables(connection)
            
            # Load dimension tables first
            load_to_dw(df_mahasiswa, "dim_mahasiswa", connection)
            load_to_dw(df_matakuliah, "dim_matakuliah", connection)
            load_to_dw(df_dosen, "dim_dosen", connection)
            load_to_dw(df_semester, "dim_semester", connection)
            
            # Then load fact tables
            load_to_dw(df_fakta_nilai, "fakta_nilai", connection)
            load_to_dw(df_ipk, "fakta_ipk", connection)
            
            # Visualize data
            visualize_data(df_ipk, df_statistik_fakultas)
            
            print("\nProses ETL berhasil diselesaikan!")
        
    except Exception as e:
        print(f"\nERROR: Terjadi kesalahan dalam proses ETL: {e}")
    
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Koneksi database ditutup")
        print("\n=== PROGRAM SELESAI ===")