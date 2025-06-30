<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\DimDosenResource\Pages;
use App\Filament\Admin\Resources\DimDosenResource\RelationManagers;
use App\Models\DimDosen;
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;

class DimDosenResource extends Resource
{
    protected static ?string $model = DimDosen::class;

    protected static ?string $navigationIcon = 'heroicon-o-user-group';
    protected static ?string $modelLabel = 'Dosen';
    protected static ?string $navigationLabel = 'Data Dosen';
    protected static ?string $navigationGroup = 'Data Akademik';
    protected static ?int $navigationSort = 2;

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Informasi Dosen')
                    ->description('Data pribadi dosen')
                    ->schema([
                        Forms\Components\TextInput::make('nidn')
                            ->required()
                            ->unique(ignoreRecord: true)
                            ->label('NIDN')
                            ->maxLength(20)
                            ->placeholder('1234567890'),

                        Forms\Components\TextInput::make('nama_dosen')
                            ->required()
                            ->maxLength(255)
                            ->columnSpanFull(),

                        Forms\Components\Select::make('jabatan')
                            ->required()
                            ->options([
                                'Asisten Ahli' => 'Asisten Ahli',
                                'Lektor' => 'Lektor',
                                'Lektor Kepala' => 'Lektor Kepala',
                                'Guru Besar' => 'Guru Besar',
                                'Tenaga Pengajar' => 'Tenaga Pengajar',
                            ])
                            ->native(false),

                        Forms\Components\Select::make('fakultas')
                            ->required()
                            ->options([
                                'Ilmu Komputer' => 'Ilmu Komputer',
                                'Kedokteran' => 'Kedokteran',
                                'Hukum' => 'Hukum',
                                'Teknik' => 'Teknik',
                                'Ekonomi' => 'Ekonomi',
                                'Sains' => 'Sains',
                            ])
                            ->native(false)
                            ->searchable(),
                    ])
                    ->columns(2),

                Forms\Components\Section::make('Informasi Tambahan')
                    ->collapsible()
                    ->schema([
                        Forms\Components\TextInput::make('gelar_depan')
                            ->maxLength(20),

                        Forms\Components\TextInput::make('gelar_belakang')
                            ->maxLength(20),

                        Forms\Components\TextInput::make('email')
                            ->email()
                            ->maxLength(100),

                        Forms\Components\TextInput::make('telepon')
                            ->tel()
                            ->maxLength(20),
                    ])
                    ->columns(2),
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('nidn')
                    ->label('NIDN')
                    ->searchable()
                    ->sortable(),

                Tables\Columns\TextColumn::make('nama_dosen')
                    ->label('Nama Dosen')
                    ->searchable()
                    ->sortable()
                    ->description(fn(DimDosen $record) => $record->gelar_depan . ' ' . $record->gelar_belakang),

                Tables\Columns\TextColumn::make('jabatan')
                    ->badge()
                    ->color(fn(string $state): string => match ($state) {
                        'Guru Besar' => 'danger',
                        'Lektor Kepala' => 'warning',
                        'Lektor' => 'primary',
                        default => 'success',
                    })
                    ->sortable(),

                Tables\Columns\TextColumn::make('fakultas')
                    ->badge()
                    ->color('info')
                    ->sortable(),

                Tables\Columns\TextColumn::make('mata_kuliah_diajar_count')
                    ->label('Jumlah MK')
                    ->counts('mataKuliahDiajar')
                    ->sortable(),

            ])
            ->defaultSort('nama_dosen', 'asc')
            ->filters([
                Tables\Filters\SelectFilter::make('fakultas')
                    ->options([
                        'Ilmu Komputer' => 'Ilmu Komputer',
                        'Kedokteran' => 'Kedokteran',
                        'Hukum' => 'Hukum',
                        'Teknik' => 'Teknik',
                        'Ekonomi' => 'Ekonomi',
                        'Sains' => 'Sains',
                    ]),

                Tables\Filters\SelectFilter::make('jabatan')
                    ->options([
                        'Asisten Ahli' => 'Asisten Ahli',
                        'Lektor' => 'Lektor',
                        'Lektor Kepala' => 'Lektor Kepala',
                        'Guru Besar' => 'Guru Besar',
                        'Tenaga Pengajar' => 'Tenaga Pengajar',
                    ]),
            ])
            ->actions([
                Tables\Actions\ActionGroup::make([
                    Tables\Actions\ViewAction::make(),
                    Tables\Actions\EditAction::make(),
                    Tables\Actions\DeleteAction::make(),
                ]),
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
                Tables\Grouping\Group::make('fakultas')
                    ->label('Fakultas')
                    ->collapsible(),

                Tables\Grouping\Group::make('jabatan')
                    ->label('Jabatan')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            //RelationManagers\MataKuliahDiajarRelationManager::class,
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListDimDosens::route('/'),
            'create' => Pages\CreateDimDosen::route('/create'),
            'edit' => Pages\EditDimDosen::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count();
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::count() > 10 ? 'primary' : 'warning';
    }
}
