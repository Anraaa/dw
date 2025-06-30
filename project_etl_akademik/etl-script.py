import pandas as pd
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
from datetime import datetime
import seaborn as sns
from matplotlib import gridspec
import random

# =============================================
# 1. EXTRACT (Mengambil Data dari Berbagai Sumber)
# =============================================
def extract_data():
    """Fungsi untuk mengekstrak data dari berbagai sumber sistem akademik"""
    print("\n[1] Extracting data from multiple sources...")

    # --- Data Mahasiswa ---
    num_students = 60 # Target 60 records
    nims = [f'10120{random.randint(100, 999)}' for _ in range(num_students)] # More varied NIMS
    nama_students = [f'Mahasiswa {i+1}' for i in range(num_students)]
    fakultas_list = ['Teknik', 'Ekonomi', 'Sains', 'Hukum', 'Kedokteran', 'Ilmu Komputer']
    prodi_mapping = {
        'Teknik': ['Informatika', 'Sistem Informasi', 'Teknik Elektro', 'Teknik Mesin'],
        'Ekonomi': ['Manajemen', 'Akuntansi', 'Ekonomi Pembangunan'],
        'Sains': ['Fisika', 'Matematika', 'Biologi', 'Kimia'],
        'Hukum': ['Hukum Perdata', 'Hukum Pidana', 'Hukum Internasional'],
        'Kedokteran': ['Pendidikan Dokter', 'Farmasi'],
        'Ilmu Komputer': ['Ilmu Komputer', 'Teknologi Informasi']
    }
    
    fakultas_mahasiswa = [random.choice(fakultas_list) for _ in range(num_students)]
    prodi_mahasiswa = [random.choice(prodi_mapping[fak]) for fak in fakultas_mahasiswa]
    tahun_masuk_mahasiswa = [random.choice([2020, 2021, 2022, 2023]) for _ in range(num_students)]
    status_beasiswa_mahasiswa = [random.choice(['Ya', 'Tidak']) for _ in range(num_students)]

    data_mahasiswa = {
        'nim': nims,
        'nama': nama_students,
        'fakultas': fakultas_mahasiswa,
        'prodi': prodi_mahasiswa,
        'tahun_masuk': tahun_masuk_mahasiswa,
        'status_beasiswa': status_beasiswa_mahasiswa
    }
    df_mahasiswa = pd.DataFrame(data_mahasiswa)

    # --- Data Mata Kuliah ---
    num_courses = 70 # Target 70 records
    kode_mk_list = []
    nama_mk_list = []
    sks_list = []
    semester_mk_list = []

    prodi_prefixes = {
        'Informatika': 'INF', 'Sistem Informasi': 'SIS', 'Teknik Elektro': 'ELE', 'Teknik Mesin': 'MES',
        'Manajemen': 'MAN', 'Akuntansi': 'AKU', 'Ekonomi Pembangunan': 'EKO',
        'Fisika': 'FIS', 'Matematika': 'MAT', 'Biologi': 'BIO', 'Kimia': 'KIM',
        'Hukum Perdata': 'HPR', 'Hukum Pidana': 'HPD', 'Hukum Internasional': 'HIN',
        'Pendidikan Dokter': 'DOK', 'Farmasi': 'FAR',
        'Ilmu Komputer': 'IKM', 'Teknologi Informasi': 'TIN'
    }

    for i in range(num_courses):
        prodi = random.choice(list(prodi_prefixes.keys()))
        prefix = prodi_prefixes[prodi]
        kode_mk = f'{prefix}{random.randint(100, 499)}'
        nama_mk = f'Mata Kuliah {prodi} {i+1}'
        sks = random.choice([2, 3, 4])
        semester_mk = random.randint(1, 8)
        
        kode_mk_list.append(kode_mk)
        nama_mk_list.append(nama_mk)
        sks_list.append(sks)
        semester_mk_list.append(semester_mk)

    data_matakuliah = {
        'kode_mk': kode_mk_list,
        'nama_mk': nama_mk_list,
        'sks': sks_list,
        'semester_mk': semester_mk_list
    }
    df_matakuliah = pd.DataFrame(data_matakuliah)

    # --- Data Dosen ---
    num_lecturers = 55 # Target 55 records
    nidn_dosen = [f'00{random.randint(1000, 9999)}' for _ in range(num_lecturers)]
    nama_dosen = [f'Dosen {i+1}' for i in range(num_lecturers)]
    jabatan_list = ['Asisten Ahli', 'Lektor', 'Lektor Kepala', 'Guru Besar']
    
    fakultas_dosen = [random.choice(fakultas_list) for _ in range(num_lecturers)]
    jabatan_dosen = [random.choice(jabatan_list) for _ in range(num_lecturers)]

    data_dosen = {
        'nidn': nidn_dosen,
        'nama_dosen': nama_dosen,
        'jabatan': jabatan_dosen,
        'fakultas': fakultas_dosen
    }
    df_dosen = pd.DataFrame(data_dosen)

    # --- Data Nilai ---
    num_grades = 100 # Target 100 records (students take multiple courses)
    nim_for_grades = random.choices(df_mahasiswa['nim'].tolist(), k=num_grades)
    kode_mk_for_grades = random.choices(df_matakuliah['kode_mk'].tolist(), k=num_grades)
    nidn_dosen_for_grades = random.choices(df_dosen['nidn'].tolist(), k=num_grades)
    
    tahun_ajaran_list = ['2021/2022', '2022/2023', '2023/2024']
    semester_list = ['Ganjil', 'Genap']
    
    nilai_akhir = [random.randint(60, 100) for _ in range(num_grades)]
    status_kelulusan = ['Lulus' if nilai >= 65 else 'Tidak Lulus' for nilai in nilai_akhir]

    data_nilai = {
        'nim': nim_for_grades,
        'kode_mk': kode_mk_for_grades,
        'nidn_dosen': nidn_dosen_for_grades,
        'tahun_ajaran': [random.choice(tahun_ajaran_list) for _ in range(num_grades)],
        'semester': [random.choice(semester_list) for _ in range(num_grades)],
        'nilai_akhir': nilai_akhir,
        'status_kelulusan': status_kelulusan
    }
    df_nilai = pd.DataFrame(data_nilai)

    # --- Data Keuangan ---
    num_financial = 65 # Target 65 records
    nim_for_finance = random.choices(df_mahasiswa['nim'].tolist(), k=num_financial)
    
    tahun_ajaran_finance = [random.choice(tahun_ajaran_list) for _ in range(num_financial)]
    semester_keuangan = [random.choice(semester_list) for _ in range(num_financial)]
    
    # Simulate different payment amounts based on 'simulated' faculty/prodi
    pembayaran_options = {
        'Teknik': 7500000, 'Ekonomi': 6500000, 'Sains': 7000000, 'Hukum': 6800000,
        'Kedokteran': 15000000, 'Ilmu Komputer': 8000000
    }
    total_pembayaran = [random.choice(list(pembayaran_options.values())) for _ in range(num_financial)]
    status_pembayaran = [random.choice(['Lunas', 'Cicilan', 'Belum Lunas']) for _ in range(num_financial)]

    data_keuangan = {
        'nim': nim_for_finance,
        'tahun_ajaran': tahun_ajaran_finance,
        'semester_keuangan': semester_keuangan,
        'total_pembayaran': total_pembayaran,
        'status_pembayaran': status_pembayaran
    }
    df_keuangan = pd.DataFrame(data_keuangan)

    print(f"Data extraction completed successfully! Loaded {len(df_mahasiswa)} student records, {len(df_matakuliah)} course records, {len(df_dosen)} lecturer records, {len(df_nilai)} grade records, and {len(df_keuangan)} financial records.")
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
    
    # 6. Menghitung statistik fakultas dengan jumlah mahasiswa beasiswa
    # Gabungkan data IPK dengan data mahasiswa terlebih dahulu
    df_mahasiswa_with_ipk = pd.merge(df_mahasiswa, df_ipk_final[['nim', 'ipk']], on='nim', how='left')
    
    df_statistik_fakultas = df_mahasiswa_with_ipk.groupby('fakultas').agg(
        jumlah_mahasiswa=('nim', 'count'),
        jumlah_beasiswa=('status_beasiswa', lambda x: (x == 'Ya').sum()),
        jumlah_tanpa_beasiswa=('status_beasiswa', lambda x: (x == 'Tidak').sum()),
        rata_ipk=('ipk', 'mean')
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
            port=3308,
            user='akademik_user',
            password='akademik123',
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
# 4. VISUALISASI (Analisis Data Akademik) - ENHANCED
# =============================================
def visualize_data(df_ipk, df_statistik_fakultas):
    """Fungsi untuk visualisasi data dengan detail per mahasiswa dan analisis beasiswa"""
    print("\n[4] Generating enhanced student-level visualizations with scholarship analysis...")
    
    try:
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        plt.rcParams['font.family'] = 'DejaVu Sans'
        colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6']
        
        # Calculate minimum IPK for scholarship
        min_ipk_beasiswa = df_ipk[df_ipk['status_beasiswa'] == 'Ya']['ipk'].min()
        
       # =============================================
        # 1. COMPREHENSIVE IPK AND SCHOLARSHIP ANALYSIS (Figure 1)
        # =============================================
        plt.figure(figsize=(20, 12)) # Increased overall figure size for main plots
        plt.suptitle('ANALISIS LENGKAP IPK DAN BEASISWA PER FAKULTAS', y=1.02, fontsize=16, fontweight='bold')
        
        # Create grid layout for Figure 1
        gs = gridspec.GridSpec(1, 2, width_ratios=[2, 1]) # Adjusted ratios for 2 plots
        ax_dist = plt.subplot(gs[0, 0])  # Distribution plot
        ax_status = plt.subplot(gs[0, 1])  # Scholarship status bar chart
        
        # --------------------------
        # Distribution Plot (Violin + Box + Swarm)
        # --------------------------
        sns.violinplot(x='fakultas', y='ipk', data=df_ipk, palette=colors, 
                      inner=None, ax=ax_dist, cut=0, width=0.8)
        sns.boxplot(x='fakultas', y='ipk', data=df_ipk, color='black',
                   width=0.15, ax=ax_dist, boxprops={'facecolor':'none'})
        sns.swarmplot(x='fakultas', y='ipk', hue='status_beasiswa',
                     data=df_ipk, palette={'Ya':'#27ae60', 'Tidak':'#e74c3c'},
                     size=5, edgecolor='gray', linewidth=0.5, ax=ax_dist)
        
        ax_dist.axhline(y=min_ipk_beasiswa, color='#f39c12', linestyle='--', linewidth=2)
        ax_dist.text(0.5, min_ipk_beasiswa+0.05, 
                f"IPK Minimum Beasiswa: {min_ipk_beasiswa:.2f}",
                color='#f39c12', fontweight='bold', ha='center')
        
        ax_dist.set_title('DISTRIBUSI IPK DAN STATUS BEASISWA\n(Violin: density, Box: quartile, Titik: mahasiswa)',
                     pad=15, fontsize=12)
        ax_dist.set_xlabel('Fakultas', fontsize=12)
        ax_dist.set_ylabel('IPK', fontsize=12)
        ax_dist.set_ylim(1.5, 4.0) # Adjusted y-limit
        ax_dist.legend(title='Status Beasiswa', bbox_to_anchor=(1, 1))
        
        # --------------------------
        # Scholarship Status Analysis
        # --------------------------
        df_eligible = df_ipk.copy()
        df_eligible['Status'] = np.where(
            df_eligible['status_beasiswa'] == 'Ya', 'Dapat',
            np.where(
                df_eligible['ipk'] < min_ipk_beasiswa,
                f'Tidak (IPK < {min_ipk_beasiswa:.2f})',
                'Tidak (Kuota)'
            )
        )
        
        status_counts = df_eligible.groupby(['fakultas', 'Status']).size().unstack()
        status_counts.plot(kind='barh', stacked=True, 
                         color=['#27ae60', '#e74c3c', '#f39c12'],
                         ax=ax_status)
        
        for i, (idx, row) in enumerate(status_counts.iterrows()):
            total = row.sum()
            cumulative = 0
            for val in row:
                if val > 0:
                    ax_status.text(cumulative + val/2, i, 
                            f"{val} ({val/total*100:.1f}%)",
                            va='center', ha='center',
                            color='white', fontweight='bold')
                    cumulative += val
        
        ax_status.set_title('DISTRIBUSI STATUS BEASISWA', pad=15, fontsize=12)
        ax_status.set_xlabel('Jumlah Mahasiswa')
        ax_status.legend(title='Keterangan')
        
        plt.tight_layout()
        plt.show()
        
        # =============================================
        # 2. DETAILED STUDENT TABLE (Figure 2)
        # =============================================
        plt.figure(figsize=(12, 10)) # Separate figure for the table
        ax_table = plt.subplot(111)
        ax_table.axis('off') # Hide axes for the table plot
        
        table_data = []
        for faculty in df_ipk['fakultas'].unique():
            faculty_data = df_ipk[df_ipk['fakultas'] == faculty]
            
            for _, row in faculty_data.iterrows():
                status = 'Dapat' if row['status_beasiswa'] == 'Ya' else (
                    f'Tidak (IPK < {min_ipk_beasiswa:.2f})' 
                    if row['ipk'] < min_ipk_beasiswa 
                    else 'Tidak (Kuota)'
                )
                color = '#2ecc71' if row['status_beasiswa'] == 'Ya' else (
                    '#e74c3c' if row['ipk'] < min_ipk_beasiswa 
                    else '#f39c12'
                )
                
                table_data.append([
                    row['nama'],
                    row['prodi'],
                    row['tahun_masuk'],
                    f"{row['ipk']:.2f}",
                    status,
                    color
                ])
        
        columns = ['Nama', 'Prodi', 'Tahun', 'IPK', 'Status'] # Removed 'color' from columns
        
        # Create table
        table = ax_table.table(
            cellText=[row[:-1] for row in table_data], # Exclude the color column for cellText
            colLabels=columns,
            cellColours=[[row[-1]]*len(columns) for row in table_data], # Apply color to all cells in a row
            loc='center',
            cellLoc='center',
            bbox=[0, 0, 1, 1] # Make table fill the entire axis area
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10) # Increased font size
        table.scale(1, 1.2) # Adjusted scale for better row height
        
        ax_table.set_title('DETAIL TABEL MAHASISWA DAN STATUS BEASISWA', pad=20, fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.show()
        
        
        # =============================================
        # 2. DETAILED SCHOLARSHIP ANALYSIS
        # =============================================
        fig = plt.figure(figsize=(18, 8))
        plt.suptitle('ANALISIS DETAIL KELULUSAN BEASISWA', y=1.02, fontsize=16, fontweight='bold')
        
        gs = gridspec.GridSpec(1, 2, width_ratios=[2, 1])
        ax1 = plt.subplot(gs[0, 0])
        ax2 = plt.subplot(gs[0, 1])
        
        # --------------------------
        # IPK vs Scholarship Status
        # --------------------------
        sns.boxplot(x='fakultas', y='ipk', hue='status_beasiswa',
                   data=df_ipk, palette={'Ya':'#27ae60', 'Tidak':'#e74c3c'},
                   ax=ax1)
        
        # Add threshold line and annotations
        ax1.axhline(y=min_ipk_beasiswa, color='#f39c12', linestyle='--', linewidth=2)
        ax1.text(0.5, min_ipk_beasiswa+0.05, 
                f"BATAS MINIMUM IPK UNTUK BEASISWA: {min_ipk_beasiswa:.2f}",
                color='#f39c12', fontweight='bold', ha='center')
        
        # Add reasons for not getting scholarship
        for i, faculty in enumerate(df_ipk['fakultas'].unique()):
            faculty_data = df_ipk[df_ipk['fakultas'] == faculty]
            tanpa_beasiswa = faculty_data[faculty_data['status_beasiswa'] == 'Tidak']
            
            below_threshold = len(tanpa_beasiswa[tanpa_beasiswa['ipk'] < min_ipk_beasiswa])
            above_threshold = len(tanpa_beasiswa[tanpa_beasiswa['ipk'] >= min_ipk_beasiswa])
            
            ax1.text(i, 1.7, 
                    f"Tidak dapat:\n"
                    f"- IPK < {min_ipk_beasiswa:.2f}: {below_threshold} mhs\n"
                    f"- Kuota penuh: {above_threshold} mhs",
                    ha='center', va='center', fontsize=9,
                    bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray'))
        
        ax1.set_title('PERBANDINGAN IPK PENERIMA DAN NON-PENERIMA BEASISWA', pad=15, fontsize=12)
        ax1.set_xlabel('Fakultas', fontsize=12)
        ax1.set_ylabel('IPK', fontsize=12)
        ax1.legend(title='Status Beasiswa')
        
        # --------------------------
        # Scholarship Eligibility Analysis
        # --------------------------
        # Prepare data
        df_eligible = df_ipk.copy()
        df_eligible['status'] = np.where(
            df_eligible['status_beasiswa'] == 'Ya', 'Dapat Beasiswa',
            np.where(
                df_eligible['ipk'] < min_ipk_beasiswa, 
                f'Tidak Lulus (IPK < {min_ipk_beasiswa:.2f})', 
                'Tidak Lulus (Kuota Penuh)'
            )
        )
        
        status_counts = df_eligible.groupby(['fakultas', 'status']).size().unstack()
        
        # Plot
        status_counts.plot(kind='barh', stacked=True, 
                         color=['#27ae60', '#e74c3c', '#f39c12'],
                         ax=ax2)
        
        # Add annotations
        for i, (idx, row) in enumerate(status_counts.iterrows()):
            total = row.sum()
            cumulative = 0
            for val in row:
                if val > 0:
                    ax2.text(cumulative + val/2, i, 
                            f"{val} ({val/total*100:.1f}%)",
                            va='center', ha='center',
                            color='white', fontweight='bold')
                    cumulative += val
        
        ax2.set_title('DETAIL STATUS KELULUSAN BEASISWA', pad=15, fontsize=12)
        ax2.set_xlabel('Jumlah Mahasiswa', fontsize=12)
        ax2.set_ylabel('Fakultas', fontsize=12)
        ax2.legend(title='Keterangan Status')
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        print(f"Error dalam visualisasi data: {e}")
        raise



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
