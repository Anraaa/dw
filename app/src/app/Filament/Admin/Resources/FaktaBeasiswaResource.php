<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\FaktaBeasiswaResource\Pages;
use App\Models\FaktaBeasiswa;
use App\Models\DimMahasiswa;
use App\Models\DimBeasiswa;
use App\Models\DimSemester;
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Filament\Tables\Filters\SelectFilter;
use Filament\Tables\Filters\Filter;
use pxlrbt\FilamentExcel\Actions\Tables\ExportAction;
use pxlrbt\FilamentExcel\Exports\ExcelExport;
use pxlrbt\FilamentExcel\Columns\Column; // Make sure this is imported!

class FaktaBeasiswaResource extends Resource
{
    protected static ?string $model = FaktaBeasiswa::class;

    protected static ?string $navigationIcon = 'heroicon-o-currency-dollar';
    protected static ?string $modelLabel = 'Alokasi Beasiswa';
    protected static ?string $pluralModelLabel = 'Alokasi Beasiswa';
    protected static ?string $navigationLabel = 'Alokasi Beasiswa';
    protected static ?int $navigationSort = 7;
    protected static ?string $navigationGroup = 'Data Akademik';

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Detail Alokasi Beasiswa')
                    ->description('Catatan rinci tentang beasiswa yang diberikan kepada mahasiswa.')
                    ->schema([
                        Forms\Components\Select::make('id_mahasiswa')
                            ->label('Mahasiswa Penerima')
                            ->relationship('mahasiswa', 'nama')
                            ->getOptionLabelUsing(fn (DimMahasiswa $record) => "{$record->nim} - {$record->nama}")
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\Select::make('id_beasiswa')
                            ->label('Jenis Beasiswa')
                            ->relationship('beasiswa', 'nama_beasiswa')
                            ->getOptionLabelUsing(fn (DimBeasiswa $record) => "{$record->nama_beasiswa} ({$record->jenis_beasiswa})")
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\Select::make('id_semester')
                            ->label('Semester Pemberian')
                            ->relationship('semester', 'tahun_ajaran')
                            ->getOptionLabelUsing(fn (DimSemester $record) => "{$record->tahun_ajaran} - {$record->semester}")
                            ->searchable()
                            ->preload()
                            ->nullable(),

                        Forms\Components\TextInput::make('ipk_saat_penerimaan')
                            ->label('IPK Saat Penerimaan')
                            ->required()
                            ->numeric()
                            ->minValue(0.00)
                            ->maxValue(4.00)
                            ->step(0.01)
                            ->placeholder('Contoh: 3.85')
                            ->suffix('IPK'),

                        Forms\Components\TextInput::make('sks_saat_penerimaan')
                            ->label('SKS Saat Penerimaan')
                            ->required()
                            ->numeric()
                            ->minValue(0)
                            ->placeholder('Contoh: 70')
                            ->suffix('SKS'),

                        Forms\Components\DatePicker::make('tanggal_penerimaan')
                            ->label('Tanggal Penerimaan')
                            ->required()
                            ->native(false)
                            ->closeOnDateSelection(),

                        Forms\Components\DatePicker::make('tanggal_berakhir')
                            ->label('Tanggal Berakhir Beasiswa')
                            ->native(false)
                            ->closeOnDateSelection()
                            ->nullable(),

                        Forms\Components\Select::make('status_pemberian')
                            ->label('Status Pemberian')
                            ->required()
                            ->options([
                                'Aktif' => 'Aktif',
                                'Non-Aktif' => 'Non-Aktif',
                                'Dicabut' => 'Dicabut',
                                'Selesai' => 'Selesai',
                            ])
                            ->default('Aktif')
                            ->native(false),

                        Forms\Components\TextInput::make('sumber_dana')
                            ->label('Sumber Dana')
                            ->maxLength(255)
                            ->placeholder('Contoh: Kementerian Pendidikan')
                            ->nullable(),

                        Forms\Components\TextInput::make('jumlah_bantuan')
                            ->label('Jumlah Bantuan (Rp)')
                            ->numeric()
                            ->minValue(0)
                            ->suffix('Rp')
                            ->placeholder('Contoh: 2500000.00')
                            ->nullable(),
                    ])
                    ->columns(2),
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('mahasiswa.nama')
                    ->label('Mahasiswa')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('beasiswa.nama_beasiswa')
                    ->label('Nama Beasiswa')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('semester.tahun_ajaran')
                    ->label('Tahun Ajaran')
                    ->sortable(),
                Tables\Columns\TextColumn::make('semester.semester')
                    ->label('Semester')
                    ->sortable(),
                Tables\Columns\TextColumn::make('ipk_saat_penerimaan')
                    ->label('IPK Saat Terima')
                    ->numeric()
                    ->sortable()
                    ->badge()
                    ->color(fn (float $state): string => match (true) {
                        $state >= 3.50 => 'success',
                        $state >= 3.00 => 'info',
                        default => 'warning',
                    }),
                Tables\Columns\TextColumn::make('sks_saat_penerimaan')
                    ->label('SKS Saat Terima')
                    ->numeric()
                    ->sortable()
                    ->suffix(' SKS'),
                Tables\Columns\TextColumn::make('tanggal_penerimaan')
                    ->label('Tanggal Terima')
                    ->date('d M Y')
                    ->sortable(),
                Tables\Columns\TextColumn::make('tanggal_berakhir')
                    ->label('Tanggal Berakhir')
                    ->date('d M Y')
                    ->sortable()
                    ->placeholder('N/A'),
                Tables\Columns\TextColumn::make('status_pemberian')
                    ->label('Status')
                    ->badge()
                    ->color(fn (string $state): string => match ($state) {
                        'Aktif' => 'success',
                        'Selesai' => 'gray',
                        'Non-Aktif' => 'warning',
                        'Dicabut' => 'danger',
                        default => 'secondary',
                    })
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('jumlah_bantuan')
                    ->label('Jumlah Bantuan')
                    ->numeric()
                    ->sortable()
                    ->money('IDR', locale: 'id')
                    ->placeholder('Tidak Ada'),
                Tables\Columns\TextColumn::make('created_at')
                    ->label('Dibuat')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
                Tables\Columns\TextColumn::make('updated_at')
                    ->label('Diperbarui')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
            ])
            ->defaultSort('tanggal_penerimaan', 'desc')
            ->filters([
                Tables\Filters\SelectFilter::make('id_mahasiswa')
                    ->label('Filter per Mahasiswa')
                    ->relationship('mahasiswa', 'nama')
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('id_beasiswa')
                    ->label('Filter per Beasiswa')
                    ->relationship('beasiswa', 'nama_beasiswa')
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('id_semester')
                    ->label('Filter per Semester')
                    ->relationship('semester', 'tahun_ajaran')
                    ->getOptionLabelUsing(fn (DimSemester $record) => "{$record->tahun_ajaran} - {$record->semester}")
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('status_pemberian')
                    ->label('Filter per Status')
                    ->options([
                        'Aktif' => 'Aktif',
                        'Non-Aktif' => 'Non-Aktif',
                        'Dicabut' => 'Dicabut',
                        'Selesai' => 'Selesai',
                    ]),
                Tables\Filters\Filter::make('ipk_criteria')
                    ->label('IPK')
                    ->form([
                        Forms\Components\TextInput::make('min_ipk')
                            ->numeric()
                            ->step(0.01)
                            ->placeholder('Min IPK'),
                        Forms\Components\TextInput::make('max_ipk')
                            ->numeric()
                            ->step(0.01)
                            ->placeholder('Max IPK'),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query
                            ->when(
                                $data['min_ipk'],
                                fn (Builder $query, $ipk): Builder => $query->where('ipk_saat_penerimaan', '>=', $ipk),
                            )
                            ->when(
                                $data['max_ipk'],
                                fn (Builder $query, $ipk): Builder => $query->where('ipk_saat_penerimaan', '<=', $ipk),
                            );
                    }),
                Tables\Filters\Filter::make('tanggal_penerimaan_range')
                    ->label('Rentang Tanggal Penerimaan')
                    ->form([
                        Forms\Components\DatePicker::make('from_date')
                            ->label('Dari Tanggal')
                            ->native(false),
                        Forms\Components\DatePicker::make('to_date')
                            ->label('Sampai Tanggal')
                            ->native(false),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query
                            ->when(
                                $data['from_date'],
                                fn (Builder $query, $date): Builder => $query->whereDate('tanggal_penerimaan', '>=', $date),
                            )
                            ->when(
                                $data['to_date'],
                                fn (Builder $query, $date): Builder => $query->whereDate('tanggal_penerimaan', '<=', $date),
                            );
                    }),
            ])
            ->headerActions([
                ExportAction::make()
                    ->exports([
                        ExcelExport::make('fakta_beasiswa')
                            ->fromModel(FaktaBeasiswa::class)
                            // Eager loading is handled in the FaktaBeasiswa model's $with property.
                            ->withFilename('alokasi_beasiswa_' . now()->format('Ymd_His'))
                            ->withWriterType(\Maatwebsite\Excel\Excel::XLSX)
                            ->withColumns([
                                Column::make('id')->heading('ID'),

                                // Mahasiswa details
                                Column::make('mahasiswa.nim')
                                    ->heading('NIM Mahasiswa')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default
                                Column::make('mahasiswa.nama')
                                    ->heading('Nama Mahasiswa')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default
                                Column::make('mahasiswa.prodi')
                                    ->heading('Prodi Mahasiswa')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default

                                // Beasiswa details
                                Column::make('beasiswa.nama_beasiswa')
                                    ->heading('Nama Beasiswa')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default
                                Column::make('beasiswa.jenis_beasiswa')
                                    ->heading('Jenis Beasiswa')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default

                                // Semester details
                                Column::make('semester.tahun_ajaran')
                                    ->heading('Tahun Ajaran')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default
                                Column::make('semester.semester')
                                    ->heading('Semester')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default

                                // Fakta Beasiswa attributes (already had formatStateUsing, but adding for completeness)
                                Column::make('ipk_saat_penerimaan')->heading('IPK Saat Terima'),
                                Column::make('sks_saat_penerimaan')->heading('SKS Saat Terima'),
                                Column::make('tanggal_penerimaan')
                                    ->heading('Tanggal Penerimaan')
                                    ->formatStateUsing(fn ($state) => $state ? \Carbon\Carbon::parse($state)->format('d M Y') : 'N/A'),
                                Column::make('tanggal_berakhir')
                                    ->heading('Tanggal Berakhir')
                                    ->formatStateUsing(fn ($state) => $state ? \Carbon\Carbon::parse($state)->format('d M Y') : 'N/A'),
                                Column::make('status_pemberian')->heading('Status Pemberian'),
                                Column::make('sumber_dana')
                                    ->heading('Sumber Dana')
                                    ->formatStateUsing(fn ($state) => $state ?? 'N/A'), // Use formatStateUsing for default
                                Column::make('jumlah_bantuan')
                                    ->heading('Jumlah Bantuan (Rp)')
                                    ->formatStateUsing(fn ($state) => $state ?? '0'), // Use formatStateUsing for default numeric '0'

                                // Timestamps
                                Column::make('created_at')
                                    ->heading('Dibuat Pada')
                                    ->formatStateUsing(fn ($state) => $state ? \Carbon\Carbon::parse($state)->format('d M Y, H:i') : 'N/A'),
                                Column::make('updated_at')
                                    ->heading('Diperbarui Pada')
                                    ->formatStateUsing(fn ($state) => $state ? \Carbon\Carbon::parse($state)->format('d M Y, H:i') : 'N/A'),
                            ]),
                    ]),
            ])
            ->actions([
                Tables\Actions\ActionGroup::make([
                    Tables\Actions\ViewAction::make(),
                    Tables\Actions\EditAction::make(),
                    Tables\Actions\DeleteAction::make(),
                ])->tooltip('Aksi'),
            ])
            ->bulkActions([
                Tables\Actions\BulkActionGroup::make([
                    Tables\Actions\DeleteBulkAction::make(),
                ]),
            ])
            ->emptyStateActions([
                Tables\Actions\CreateAction::make(),
            ])
            ->groups([
                Tables\Grouping\Group::make('beasiswa.nama_beasiswa')
                    ->label('Berdasarkan Beasiswa')
                    ->collapsible(),
                Tables\Grouping\Group::make('status_pemberian')
                    ->label('Status Pemberian')
                    ->collapsible(),
                Tables\Grouping\Group::make('semester.tahun_ajaran')
                    ->label('Tahun Ajaran')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListFaktaBeasiswas::route('/'),
            'create' => Pages\CreateFaktaBeasiswa::route('/create'),
            'edit' => Pages\EditFaktaBeasiswa::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count();
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::where('status_pemberian', 'Aktif')->count() > 0 ? 'success' : 'gray';
    }
}