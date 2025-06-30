<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Models\DimMahasiswa;
use App\Models\DimMataKuliah;
use App\Models\DimDosen;
use App\Models\DimSemester;
use App\Models\DimBeasiswa;
use App\Models\FaktaNilai;
use App\Models\FaktaIpk;
use App\Models\FaktaBeasiswa;
use Illuminate\Support\Facades\DB;
use Faker\Factory as Faker;
use Carbon\Carbon;
use Illuminate\Database\Eloquent\Builder;

class AcademicETL extends Command
{
    protected $signature = 'etl:academic';
    protected $description = 'Proses ETL untuk gudang data akademik dengan alokasi beasiswa berdasarkan kriteria & kapasitas.';

    public function handle()
    {
        $this->info('Memulai proses ETL...');

        $faker = Faker::create('id_ID');

        // --- 1. Persiapan Database (Truncate dan Nonaktifkan Foreign Key) ---
        DB::statement('SET FOREIGN_KEY_CHECKS=0;');
        $this->info('Mengosongkan data yang ada...');

        DimMahasiswa::truncate();
        DimMataKuliah::truncate();
        DimDosen::truncate();
        DimSemester::truncate();
        DimBeasiswa::truncate();
        FaktaNilai::truncate();
        FaktaIpk::truncate();
        FaktaBeasiswa::truncate();

        // --- Data Konsisten untuk Fakultas dan Prodi ---
        $listFakultas = ['Ilmu Komputer', 'Teknik', 'Ekonomi dan Bisnis', 'Hukum', 'Kedokteran', 'Ilmu Sosial dan Ilmu Politik'];
        $listProdi = [
            'Ilmu Komputer' => ['Teknik Informatika', 'Sistem Informasi'],
            'Teknik' => ['Teknik Sipil', 'Teknik Elektro', 'Arsitektur'],
            'Ekonomi dan Bisnis' => ['Manajemen', 'Akuntansi', 'Ilmu Ekonomi'],
            'Hukum' => ['Ilmu Hukum'],
            'Kedokteran' => ['Pendidikan Dokter', 'Farmasi'],
            'Ilmu Sosial dan Ilmu Politik' => ['Kriminologi', 'Ilmu Komunikasi'],
        ];

        // --- 2. Ekstraksi dan Transformasi Data Dimensi (Extended Sample Data) ---
        $this->info('Mengekstrak dan mentransformasi data dimensi...');

        $mahasiswaData = [];
        for ($i = 0; $i < 150; $i++) {
            $randomFakultas = $faker->randomElement($listFakultas);
            $randomProdi = $faker->randomElement($listProdi[$randomFakultas]);

            $mahasiswaData[] = [
                'nim' => '20' . $faker->numberBetween(20, 24) . str_pad(($i + 1), 7, '0', STR_PAD_LEFT),
                'nama' => $faker->name,
                'fakultas' => $randomFakultas,
                'prodi' => $randomProdi,
                'tahun_masuk' => $faker->numberBetween(2020, 2024),
                'status_beasiswa' => 'Tidak',
            ];
        }

        $matakuliahData = [];
        for ($i = 0; $i < 40; $i++) {
            $randomSks = $faker->randomElement([2, 3, 4]);
            $randomSemesterMk = $faker->numberBetween(1, 8);
            $matakuliahData[] = [
                'kode_mk' => 'MK' . str_pad(($i + 1), 3, '0', STR_PAD_LEFT),
                'nama_mk' => $faker->unique()->words(rand(2, 4), true) . ' ' . $randomSks . ' SKS',
                'sks' => $randomSks,
                'semester_mk' => $randomSemesterMk,
            ];
        }

        $dosenData = [];
        for ($i = 0; $i < 25; $i++) {
            $randomFakultasDosen = $faker->randomElement($listFakultas);
            $dosenData[] = [
                'nidn' => str_pad(($i + 1), 6, '0', STR_PAD_LEFT),
                'nama_dosen' => $faker->name('male'),
                'jabatan' => $faker->randomElement(['Asisten Ahli', 'Lektor', 'Lektor Kepala', 'Guru Besar', 'Tenaga Pengajar']),
                'fakultas' => $randomFakultasDosen,
                'gelar_depan' => $faker->randomElement(['Dr.', 'Prof. Dr.', null]),
                'gelar_belakang' => $faker->randomElement([', M.Kom.', ', S.T., M.Eng.', ', M.H.', ', Sp.A.', null]),
                'email' => $faker->unique()->safeEmail,
                'telepon' => $faker->phoneNumber,
            ];
        }

        $semesterData = [];
        $tahunAjaranAwal = 2020;
        $tahunAjaranAkhir = 2024;
        for ($tahun = $tahunAjaranAwal; $tahun <= $tahunAjaranAkhir; $tahun++) {
            $ta = $tahun . '/' . ($tahun + 1);
            $semesterData[] = ['tahun_ajaran' => $ta, 'semester' => 'Ganjil'];
            $semesterData[] = ['tahun_ajaran' => $ta, 'semester' => 'Genap'];
            if ($faker->boolean(20)) {
                $semesterData[] = ['tahun_ajaran' => $ta, 'semester' => 'Pendek'];
            }
        }

        $beasiswaData = [
            [
                'nama_beasiswa' => 'Beasiswa Prestasi Nasional (Kedokteran)',
                'jenis_beasiswa' => 'Prestasi',
                'min_ipk_kriteria' => 3.90,
                'min_sks_kriteria' => 80,
                'kapasitas_slot' => 3,
                'tanggal_mulai_pendaftaran' => Carbon::parse('2024-01-01'),
                'tanggal_tutup_pendaftaran' => Carbon::parse('2024-02-28'),
                'is_aktif' => true,
                'deskripsi' => 'Beasiswa khusus mahasiswa Kedokteran berprestasi nasional.',
                'target_fakultas' => 'Kedokteran',
                'target_prodi' => null,
            ],
            [
                'nama_beasiswa' => 'Beasiswa Unggulan Fakultas Ilmu Komputer',
                'jenis_beasiswa' => 'Prestasi',
                'min_ipk_kriteria' => 3.60,
                'min_sks_kriteria' => 70,
                'kapasitas_slot' => 7,
                'tanggal_mulai_pendaftaran' => Carbon::parse('2024-03-01'),
                'tanggal_tutup_pendaftaran' => Carbon::parse('2024-04-30'),
                'is_aktif' => true,
                'deskripsi' => 'Beasiswa untuk mahasiswa Ilmu Komputer terbaik.',
                'target_fakultas' => 'Ilmu Komputer',
                'target_prodi' => null,
            ],
            [
                'nama_beasiswa' => 'Beasiswa Bantuan UKT Universitas',
                'jenis_beasiswa' => 'Bantuan Ekonomi',
                'min_ipk_kriteria' => 2.80,
                'min_sks_kriteria' => 50,
                'kapasitas_slot' => 20,
                'tanggal_mulai_pendaftaran' => Carbon::parse('2024-05-01'),
                'tanggal_tutup_pendaftaran' => Carbon::parse('2024-06-30'),
                'is_aktif' => true,
                'deskripsi' => 'Beasiswa untuk mahasiswa dengan kendala ekonomi dari berbagai fakultas.',
                'target_fakultas' => null,
                'target_prodi' => null,
            ],
            [
                'nama_beasiswa' => 'Beasiswa Teknik Informatika Berbakat',
                'jenis_beasiswa' => 'Prestasi',
                'min_ipk_kriteria' => 3.40,
                'min_sks_kriteria' => 60,
                'kapasitas_slot' => 5,
                'tanggal_mulai_pendaftaran' => Carbon::parse('2024-07-01'),
                'tanggal_tutup_pendaftaran' => Carbon::parse('2024-08-31'),
                'is_aktif' => true,
                'deskripsi' => 'Beasiswa khusus mahasiswa Teknik Informatika dengan IPK tinggi.',
                'target_fakultas' => 'Ilmu Komputer',
                'target_prodi' => 'Teknik Informatika',
            ],
            // Beasiswa Tanpa Batas Slot - Akan dilewati dari alokasi berbasis slot
            [
                'nama_beasiswa' => 'Beasiswa Kemitraan Khusus',
                'jenis_beasiswa' => 'Kerja Sama',
                'min_ipk_kriteria' => 3.00,
                'min_sks_kriteria' => 40,
                'kapasitas_slot' => 0, // Kapasitas 0 = tak terbatas
                'tanggal_mulai_pendaftaran' => Carbon::parse('2024-01-01'),
                'tanggal_tutup_pendaftaran' => Carbon::parse('2024-12-31'),
                'is_aktif' => true,
                'deskripsi' => 'Beasiswa untuk program kemitraan.',
                'target_fakultas' => null,
                'target_prodi' => null,
            ],
            [
                'nama_beasiswa' => 'Beasiswa Dosen Muda',
                'jenis_beasiswa' => 'Internal',
                'min_ipk_kriteria' => 3.00,
                'min_sks_kriteria' => 0,
                'kapasitas_slot' => 0, // Kapasitas 0 = tak terbatas
                'tanggal_mulai_pendaftaran' => null,
                'tanggal_tutup_pendaftaran' => null,
                'is_aktif' => false, // Non-aktifkan untuk simulasi
                'deskripsi' => 'Beasiswa khusus untuk pengembangan dosen muda.',
                'target_fakultas' => null,
                'target_prodi' => null,
            ],
        ];


        // --- 3. Memuat Tabel Dimensi ---
        $this->info('Memuat tabel dimensi...');

        DimMahasiswa::insert($mahasiswaData);
        DimMataKuliah::insert($matakuliahData);
        DimDosen::insert($dosenData);
        DimSemester::insert($semesterData);
        DimBeasiswa::insert($beasiswaData);

        // Ambil ID dari dimensi yang baru dibuat untuk digunakan di fakta
        $allMahasiswas = DimMahasiswa::all();
        $allMatakuliahs = DimMataKuliah::all();
        $allDosens = DimDosen::all();
        $allSemesters = DimSemester::all();
        $allBeasiswas = DimBeasiswa::all();

        // --- 4. Memuat Tabel FaktaNilai (Menghasilkan banyak data) ---
        $this->info('Memuat tabel FaktaNilai...');
        $nilaiFaktaEntries = [];
        $targetNilaiEntries = 1500;
        $currentNilaiCount = 0;

        if ($allMahasiswas->isEmpty() || $allMatakuliahs->isEmpty() || $allDosens->isEmpty() || $allSemesters->isEmpty()) {
            $this->error('Tidak ada data di tabel dimensi, tidak bisa membuat data FaktaNilai.');
            DB::statement('SET FOREIGN_KEY_CHECKS=1;');
            return;
        }

        while ($currentNilaiCount < $targetNilaiEntries) {
            $randomMahasiswa = $allMahasiswas->random();
            $randomMatakuliah = $allMatakuliahs->random();
            $randomDosen = $allDosens->random();

            // --- Logika yang Disesuaikan: Menentukan Semester untuk FaktaNilai ---
            // Tentukan tahun ajaran akademik berdasarkan tahun masuk mahasiswa dan progres mata kuliah
            $academicYearOffset = floor(($randomMatakuliah->semester_mk - 1) / 2); // 1-2 -> offset 0, 3-4 -> offset 1, dst.
            $courseAcademicYearStart = $randomMahasiswa->tahun_masuk + $academicYearOffset;
            $targetAcademicYearString = $courseAcademicYearStart . '/' . ($courseAcademicYearStart + 1);

            // Tentukan tipe semester (Ganjil/Genap) berdasarkan semester nominal mata kuliah
            $targetSemesterType = ($randomMatakuliah->semester_mk % 2 !== 0) ? 'Ganjil' : 'Genap';

            // Cari objek DimSemester yang sesuai dengan tahun ajaran dan tipe semester yang konsisten
            $consistentSemester = $allSemesters->first(function($s) use ($targetAcademicYearString, $targetSemesterType) {
                return $s->tahun_ajaran === $targetAcademicYearString && $s->semester === $targetSemesterType;
            });

            // Fallback jika tidak ditemukan (seharusnya jarang terjadi jika data semester lengkap)
            $selectedSemesterForNilai = $consistentSemester ?? $allSemesters->random();
            // --- Akhir Logika Penentuan Semester ---


            $nilaiAkhir = $faker->numberBetween(60, 100);
            if ($faker->boolean(10)) { // 10% kemungkinan dapat nilai sangat tinggi
                $nilaiAkhir = $faker->numberBetween(90, 100);
            }
            if ($faker->boolean(5) && $randomMahasiswa->fakultas == 'Kedokteran') {
                $nilaiAkhir = $faker->numberBetween(95, 100);
            }

            $statusKelulusan = ($nilaiAkhir >= 50) ? 'Lulus' : $faker->randomElement(['Tidak Lulus', 'Mengulang']);

            $nilaiFaktaEntries[] = [
                'id_mahasiswa' => $randomMahasiswa->id_mahasiswa,
                'id_matakuliah' => $randomMatakuliah->id_matakuliah,
                'id_dosen' => $randomDosen->id_dosen,
                'id_semester' => $selectedSemesterForNilai->id_semester, // Gunakan semester yang sudah ditentukan secara konsisten
                'nilai_akhir' => $nilaiAkhir,
                'status_kelulusan' => $statusKelulusan,
                'created_at' => now(),
                'updated_at' => now(),
            ];
            $currentNilaiCount++;
        }

        foreach (array_chunk($nilaiFaktaEntries, 500) as $chunk) {
            FaktaNilai::insert($chunk);
        }

        // --- 5. Memuat Tabel FaktaIpk (Berdasarkan FaktaNilai) ---
        $this->info('Menghitung dan memuat tabel FaktaIpk...');

        foreach ($allMahasiswas as $mahasiswa) {
            $nilaisMahasiswa = FaktaNilai::where('id_mahasiswa', $mahasiswa->id_mahasiswa)
                                         ->with('matakuliah')
                                         ->get();

            $totalPoint = 0;
            $totalSks = 0;

            foreach ($nilaisMahasiswa as $nilai) {
                if ($nilai->matakuliah) {
                    $gradePoint = $this->convertToGradePoint($nilai->nilai_akhir);
                    $totalPoint += $gradePoint * $nilai->matakuliah->sks;
                    $totalSks += $nilai->matakuliah->sks;
                }
            }

            $ipk = $totalSks > 0 ? round($totalPoint / $totalSks, 2) : 0.00;

            FaktaIpk::create([
                'id_mahasiswa' => $mahasiswa->id_mahasiswa,
                'total_point' => $totalPoint,
                'total_sks' => $totalSks,
                'ipk' => $ipk
            ]);
        }

        // --- 6. Alokasi Beasiswa (FaktaBeasiswa) ---
        $this->info('Melakukan alokasi beasiswa berdasarkan kriteria dan kapasitas...'); // Ubah deskripsi
        $allocatedScholarshipsCount = 0;
        // Tidak lagi menggunakan targetAllocationCount sebagai penghenti utama
        // $totalMahasiswa = DimMahasiswa::count();
        // $targetAllocationCount = round($totalMahasiswa * 0.40);

        // Kumpulkan semua mahasiswa yang memiliki data IPK, diurutkan berdasarkan IPK tertinggi
        // Ini akan jadi pool kandidat utama
        $potentialScholarshipRecipients = DimMahasiswa::whereHas('ipk')
                                            ->with('ipk')
                                            ->get()
                                            ->filter(fn($mahasiswa) => $mahasiswa->ipk !== null)
                                            ->sortByDesc(fn($mahasiswa) => $mahasiswa->ipk->ipk);

        $alreadyAllocatedMahasiswaIds = []; // Untuk melacak mahasiswa yang sudah dapat beasiswa (dari jenis apapun)

        // Loop melalui setiap jenis beasiswa yang terdefinisi
        foreach ($allBeasiswas as $beasiswa) {
            // Lewati beasiswa yang tidak aktif
            if (!$beasiswa->is_aktif) {
                $this->info("   - Beasiswa '{$beasiswa->nama_beasiswa}' tidak aktif, dilewati.");
                continue;
            }

            // Kapasitas slot 0 berarti tak terbatas, tidak akan mengisi slot yang terbatas
            // Kita tetap memproses beasiswa ini jika memiliki kapasitas 0, tapi tidak memberlakukan batas slot
            $beasiswaHasLimitedCapacity = ($beasiswa->kapasitas_slot > 0);
            
            $this->info("   - Memproses beasiswa: {$beasiswa->nama_beasiswa} (Kapasitas: " . ($beasiswaHasLimitedCapacity ? $beasiswa->kapasitas_slot : 'Tak Terbatas') . ")");

            // Filter mahasiswa yang memenuhi kriteria untuk jenis beasiswa ini
            $eligibleForThisBeasiswa = $potentialScholarshipRecipients
                ->filter(function ($mahasiswa) use ($beasiswa, $alreadyAllocatedMahasiswaIds) {
                    // Cek jika mahasiswa sudah dialokasikan beasiswa dari jenis lain dalam siklus ini
                    // PRIORITAS: Mahasiswa hanya boleh mendapat SATU beasiswa dari proses alokasi ini.
                    if (in_array($mahasiswa->id_mahasiswa, $alreadyAllocatedMahasiswaIds)) {
                        // $this->info("     - Mahasiswa '{$mahasiswa->nama}' sudah menerima beasiswa lain, dilewati."); // Debugging lebih detail
                        return false;
                    }

                    // Pastikan mahasiswa memenuhi kriteria IPK dan SKS minimum
                    if ($mahasiswa->ipk->total_sks < $beasiswa->min_sks_kriteria ||
                        $mahasiswa->ipk->ipk < $beasiswa->min_ipk_kriteria) {
                        return false;
                    }

                    // Kriteria tambahan: target fakultas/prodi (jika ada)
                    if ($beasiswa->target_fakultas && $mahasiswa->fakultas !== $beasiswa->target_fakultas) {
                        return false;
                    }
                    if ($beasiswa->target_prodi && $mahasiswa->prodi !== $beasiswa->target_prodi) {
                        return false;
                    }

                    return true; // Mahasiswa ini eligible untuk beasiswa ini
                });
            
            $this->info("     - Jumlah yang memenuhi kriteria untuk '{$beasiswa->nama_beasiswa}': " . $eligibleForThisBeasiswa->count());

            $currentSlot = 0; // Slot yang sudah terisi untuk jenis beasiswa ini
            foreach ($eligibleForThisBeasiswa as $mahasiswa) {
                // Hentikan alokasi jika kapasitas terbatas sudah penuh
                if ($beasiswaHasLimitedCapacity && $currentSlot >= $beasiswa->kapasitas_slot) {
                    $this->info("     - Kapasitas beasiswa '{$beasiswa->nama_beasiswa}' penuh.");
                    break; // Keluar dari loop mahasiswa untuk beasiswa ini
                }

                // Cek apakah mahasiswa ini sudah menerima alokasi beasiswa ini di FaktaBeasiswa (riwayat)
                // Ini penting jika proses ETL dijalankan berulang, agar tidak duplikasi alokasi untuk beasiswa yang sama
                $alreadyReceivedThisSpecificBeasiswa = FaktaBeasiswa::where('id_mahasiswa', $mahasiswa->id_mahasiswa)
                                                                    ->where('id_beasiswa', $beasiswa->id_beasiswa)
                                                                    ->where('status_pemberian', 'Aktif') // Hanya cek yang masih aktif
                                                                    ->exists();

                if ($alreadyReceivedThisSpecificBeasiswa) {
                    $this->info("     - Mahasiswa '{$mahasiswa->nama}' sudah memiliki alokasi beasiswa ini, dilewati.");
                    continue; // Lanjutkan ke mahasiswa berikutnya
                }

                // Jika lolos semua pengecekan, alokasikan beasiswa
                FaktaBeasiswa::create([
                    'id_mahasiswa' => $mahasiswa->id_mahasiswa,
                    'id_beasiswa' => $beasiswa->id_beasiswa,
                    'id_semester' => $allSemesters->random()->id_semester, // Semester pemberian bisa acak atau berdasarkan semester saat ini
                    'ipk_saat_penerimaan' => $mahasiswa->ipk->ipk,
                    'sks_saat_penerimaan' => $mahasiswa->ipk->total_sks,
                    'tanggal_penerimaan' => now(),
                    'tanggal_berakhir' => now()->addYear(), // Contoh: beasiswa berlaku 1 tahun
                    'status_pemberian' => 'Aktif',
                    'sumber_dana' => $beasiswa->jenis_beasiswa,
                    'jumlah_bantuan' => $faker->randomFloat(2, 1000000, 5000000),
                ]);

                // Perbarui status beasiswa di DimMahasiswa HANYA JIKA BELUM 'Ya'
                if ($mahasiswa->status_beasiswa !== 'Ya') {
                    $mahasiswa->update(['status_beasiswa' => 'Ya']);
                }
                
                $alreadyAllocatedMahasiswaIds[] = $mahasiswa->id_mahasiswa; // Tandai mahasiswa ini sudah dapat beasiswa (dari jenis apapun)
                $currentSlot++; // Tambah slot terisi untuk beasiswa ini
                $allocatedScholarshipsCount++; // Tambah total alokasi global

                $this->info("     - '{$mahasiswa->nama}' mendapatkan '{$beasiswa->nama_beasiswa}' (IPK: {$mahasiswa->ipk->ipk})");

                // --- Hapus Penghentian Global: Karena kita ingin mengalokasikan sebanyak mungkin ---
                // if ($allocatedScholarshipsCount >= $targetAllocationCount) {
                //     $this->info("Target alokasi beasiswa (~40%) tercapai. Menghentikan proses.");
                //     break 2; // Berhenti dari kedua loop (inner dan outer)
                // }
                // ---
            }
        }

        // --- 7. Mengaktifkan Kembali Pengecekan Foreign Key ---
        DB::statement('SET FOREIGN_KEY_CHECKS=1;');

        $this->info('Proses ETL selesai dengan sukses!');
        $this->info('Jumlah total Mahasiswa: ' . DimMahasiswa::count());
        $this->info('Jumlah total Mata Kuliah: ' . DimMataKuliah::count());
        $this->info('Jumlah total Dosen: ' . DimDosen::count());
        $this->info('Jumlah total Semester: ' . DimSemester::count());
        $this->info('Jumlah total Jenis Beasiswa: ' . DimBeasiswa::count());
        $this->info('Jumlah total Nilai: ' . FaktaNilai::count());
        $this->info('Jumlah total IPK: ' . FaktaIpk::count());
        $this->info('Jumlah total Alokasi Beasiswa: ' . FaktaBeasiswa::count()); // Langsung hitung total alokasi
        $this->info('Mahasiswa dengan Status Beasiswa "Ya": ' . DimMahasiswa::where('status_beasiswa', 'Ya')->count());
    }

    // --- Fungsi Pembantu: Konversi Nilai Angka ke Grade Point ---
    private function convertToGradePoint($nilai)
    {
        if ($nilai >= 85) return 4.0;
        if ($nilai >= 80) return 3.7;
        if ($nilai >= 75) return 3.3;
        if ($nilai >= 70) return 3.0;
        if ($nilai >= 65) return 2.7;
        if ($nilai >= 60) return 2.3;
        if ($nilai >= 55) return 2.0;
        if ($nilai >= 50) return 1.7;
        if ($nilai >= 45) return 1.3;
        if ($nilai >= 40) return 1.0;
        return 0.0;
    }
}