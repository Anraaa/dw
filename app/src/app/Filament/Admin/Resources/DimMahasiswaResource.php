<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\DimMahasiswaResource\Pages;
use App\Filament\Admin\Resources\DimMahasiswaResource\RelationManagers;
use App\Models\DimMahasiswa;
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope; // Keep this if using soft deletes

class DimMahasiswaResource extends Resource
{
    protected static ?string $model = DimMahasiswa::class;

    protected static ?string $navigationIcon = 'heroicon-o-academic-cap'; // Icon for students
    protected static ?string $modelLabel = 'Mahasiswa'; // Custom singular label
    protected static ?string $pluralModelLabel = 'Mahasiswa'; // Custom plural label
    protected static ?string $navigationLabel = 'Data Mahasiswa'; // Custom navigation label
    protected static ?string $navigationGroup = 'Data Akademik'; // Group with Dosen if relevant
    protected static ?int $navigationSort = 1; // Order within the group

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Informasi Utama Mahasiswa')
                    ->description('Data dasar dan identitas mahasiswa')
                    ->schema([
                        Forms\Components\TextInput::make('nim')
                            ->label('NIM')
                            ->required()
                            ->unique(ignoreRecord: true)
                            ->maxLength(255)
                            ->placeholder('Contoh: 20230101001'),

                        Forms\Components\TextInput::make('nama')
                            ->label('Nama Lengkap')
                            ->required()
                            ->maxLength(255)
                            ->columnSpanFull(), // Make nama field span full width

                        Forms\Components\Select::make('fakultas')
                            ->label('Fakultas')
                            ->required()
                            ->options([
                                'Fakultas Ilmu Komputer' => 'Fakultas Ilmu Komputer',
                                'Fakultas Kedokteran' => 'Fakultas Kedokteran',
                                'Fakultas Hukum' => 'Fakultas Hukum',
                                'Fakultas Teknik' => 'Fakultas Teknik',
                                'Fakultas Ekonomi dan Bisnis' => 'Fakultas Ekonomi dan Bisnis',
                                'Fakultas Ilmu Sosial dan Ilmu Politik' => 'Fakultas Ilmu Sosial dan Ilmu Politik',
                                // Add more faculties as per your data
                            ])
                            ->native(false)
                            ->searchable(), // Allows searching through faculty options

                        Forms\Components\TextInput::make('prodi')
                            ->label('Program Studi')
                            ->required()
                            ->maxLength(255), // You might want to make this a Select too for consistency

                        Forms\Components\TextInput::make('tahun_masuk')
                            ->label('Tahun Masuk')
                            ->required()
                            ->numeric()
                            ->minValue(1900) // Sensible minimum
                            ->maxValue(date('Y')) // No future year entries
                            ->placeholder('Contoh: 2023'),
                    ])
                    ->columns(2), // Arrange these fields in 2 columns

                Forms\Components\Section::make('Status Mahasiswa')
                    ->description('Informasi tambahan terkait status beasiswa')
                    ->collapsible() // Make this section collapsible
                    ->schema([
                        Forms\Components\Select::make('status_beasiswa')
                            ->label('Status Beasiswa')
                            ->required()
                            ->options([
                                'Ya' => 'Ya (Menerima Beasiswa)',
                                'Tidak' => 'Tidak (Tidak Menerima Beasiswa)',
                            ])
                            ->default('Tidak') // Default to 'Tidak'
                            ->native(false),
                    ])
                    ->columns(1), // This section will have 1 column
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('nim')
                    ->label('NIM')
                    ->searchable()
                    ->sortable(),

                Tables\Columns\TextColumn::make('nama')
                    ->label('Nama Lengkap')
                    ->searchable()
                    ->sortable(),

                Tables\Columns\TextColumn::make('fakultas')
                    ->label('Fakultas')
                    ->badge() // Display as a badge
                    ->color('info') // Example color
                    ->searchable()
                    ->sortable(),

                Tables\Columns\TextColumn::make('prodi')
                    ->label('Program Studi')
                    ->searchable()
                    ->sortable(),

                Tables\Columns\TextColumn::make('tahun_masuk')
                    ->label('Tahun Masuk')
                    ->numeric()
                    ->sortable(),

                Tables\Columns\IconColumn::make('status_beasiswa')
                    ->label('Beasiswa')
                    ->boolean() // Assumes 'Ya'/'Tidak' can be cast to boolean in model or accessor
                    ->trueIcon('heroicon-o-check-circle')
                    ->falseIcon('heroicon-o-x-circle')
                    ->trueColor('success')
                    ->falseColor('danger')
                    ->sortable(),

                // You can add a count for related IPK records if you have a relationship
                // Tables\Columns\TextColumn::make('fakta_ipks_count')
                //     ->label('Jumlah IPK Semester')
                //     ->counts('faktaIpks') // Assuming 'faktaIpks' is the relationship name
                //     ->sortable(),

                Tables\Columns\TextColumn::make('created_at')
                    ->label('Dibuat')
                    ->dateTime('d M Y H:i') // Custom format
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true), // Hidden by default

                Tables\Columns\TextColumn::make('updated_at')
                    ->label('Diperbarui')
                    ->dateTime('d M Y H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
            ])
            ->defaultSort('tahun_masuk', 'desc') // Default sort: newest students first
            ->filters([
                Tables\Filters\SelectFilter::make('fakultas')
                    ->label('Filter per Fakultas')
                    ->options(DimMahasiswa::distinct()->pluck('fakultas', 'fakultas'))
                    ->searchable(), // Make filter options searchable

                Tables\Filters\SelectFilter::make('prodi')
                    ->label('Filter per Program Studi')
                    ->options(DimMahasiswa::distinct()->pluck('prodi', 'prodi'))
                    ->searchable(),

                Tables\Filters\SelectFilter::make('status_beasiswa')
                    ->label('Filter per Beasiswa')
                    ->options([
                        'Ya' => 'Penerima Beasiswa',
                        'Tidak' => 'Bukan Penerima Beasiswa',
                    ]),

                Tables\Filters\Filter::make('tahun_masuk')
                    ->label('Filter per Tahun Masuk')
                    ->form([
                        Forms\Components\TextInput::make('from_year')
                            ->numeric()
                            ->placeholder('Dari Tahun (misal: 2020)'),
                        Forms\Components\TextInput::make('to_year')
                            ->numeric()
                            ->placeholder('Sampai Tahun (misal: 2023)'),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query
                            ->when(
                                $data['from_year'],
                                fn (Builder $query, $year): Builder => $query->where('tahun_masuk', '>=', $year),
                            )
                            ->when(
                                $data['to_year'],
                                fn (Builder $query, $year): Builder => $query->where('tahun_masuk', '<=', $year),
                            );
                    }),
                // Tables\Filters\TrashedFilter::make(), // Uncomment this if DimMahasiswa uses soft deletes
            ])
            ->actions([
                Tables\Actions\ActionGroup::make([ // Group actions into a dropdown
                    Tables\Actions\ViewAction::make(),
                    Tables\Actions\EditAction::make(),
                    Tables\Actions\DeleteAction::make(),
                ])->tooltip('Aksi'), // Tooltip for the action group
            ])
            ->bulkActions([
                Tables\Actions\BulkActionGroup::make([
                    Tables\Actions\DeleteBulkAction::make(),
                ]),
            ])
            ->emptyStateActions([ // Actions when the table is empty
                Tables\Actions\CreateAction::make(),
            ])
            ->groups([ // Grouping options for the table
                Tables\Grouping\Group::make('fakultas')
                    ->label('Fakultas')
                    ->collapsible(), // Make groups collapsible

                Tables\Grouping\Group::make('tahun_masuk')
                    ->label('Tahun Masuk')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            // If you have a relationship to FaktulIpk, you might add it here
            // RelationManagers\FaktaIpksRelationManager::class,
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListDimMahasiswas::route('/'),
            'create' => Pages\CreateDimMahasiswa::route('/create'),
            'edit' => Pages\EditDimMahasiswa::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count(); // Displays total count in navigation
    }

    public static function getNavigationBadgeColor(): ?string
    {
        // Example: Change color based on total student count
        return static::getModel()::count() > 100 ? 'success' : 'warning';
    }
}