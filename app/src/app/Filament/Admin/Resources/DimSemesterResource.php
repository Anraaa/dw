<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\DimSemesterResource\Pages;
use App\Filament\Admin\Resources\DimSemesterResource\RelationManagers;
use App\Models\DimSemester;
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;
// use Filament\Tables\Filters\TextInputFilter; // <<< HAPUS BARIS INI
use Filament\Tables\Filters\SelectFilter;   // Pastikan ini ada
use Filament\Tables\Filters\Filter;         // <<< PASTIKAN INI ADA (untuk filter kustom)
use Filament\Tables\Filters\QueryBuilder;   // Pastikan ini ada (jika Anda ingin QueryBuilder)

class DimSemesterResource extends Resource
{
    protected static ?string $model = DimSemester::class;

    protected static ?string $navigationIcon = 'heroicon-o-calendar-days';
    protected static ?string $modelLabel = 'Semester';
    protected static ?string $pluralModelLabel = 'Semester';
    protected static ?string $navigationLabel = 'Data Semester';
    protected static ?string $navigationGroup = 'Data Akademik';
    protected static ?int $navigationSort = 4;

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Informasi Semester')
                    ->description('Detail untuk Tahun Ajaran dan Semester')
                    ->schema([
                        Forms\Components\TextInput::make('tahun_ajaran')
                            ->label('Tahun Ajaran')
                            ->required()
                            ->maxLength(255)
                            ->unique(ignoreRecord: true, modifyRuleUsing: function (\Closure $rule, Forms\Get $get) {
                                return $rule->where('semester', $get('semester'));
                            })
                            ->placeholder('Contoh: 2023/2024'),

                        Forms\Components\Select::make('semester')
                            ->label('Nama Semester')
                            ->required()
                            ->options([
                                'Ganjil' => 'Ganjil',
                                'Genap' => 'Genap',
                                'Pendek' => 'Pendek',
                            ])
                            ->native(false)
                            ->unique(ignoreRecord: true, modifyRuleUsing: function (\Closure $rule, Forms\Get $get) {
                                return $rule->where('tahun_ajaran', $get('tahun_ajaran'));
                            }),
                    ])
                    ->columns(2),
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('tahun_ajaran')
                    ->label('Tahun Ajaran')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('semester')
                    ->label('Semester')
                    ->badge()
                    ->color(fn (string $state): string => match ($state) {
                        'Ganjil' => 'primary',
                        'Genap' => 'info',
                        'Pendek' => 'warning',
                        default => 'secondary',
                    })
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('created_at')
                    ->label('Dibuat Pada')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
                Tables\Columns\TextColumn::make('updated_at')
                    ->label('Diperbarui Pada')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
            ])
            ->defaultSort('tahun_ajaran', 'desc')
            ->filters([
                Tables\Filters\SelectFilter::make('semester')
                    ->label('Filter per Semester')
                    ->options([
                        'Ganjil' => 'Ganjil',
                        'Genap' => 'Genap',
                        'Pendek' => 'Pendek',
                    ]),

                // --- BAGIAN INI YANG DIUBAH ---
                Tables\Filters\Filter::make('tahun_ajaran') // Menggunakan Filter umum
                    ->label('Filter per Tahun Ajaran')
                    ->form([
                        Forms\Components\TextInput::make('search_tahun_ajaran') // TextInput di dalam form filter
                            ->placeholder('Misal: 2023/2024'),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query
                            ->when(
                                $data['search_tahun_ajaran'], // Menggunakan nama field dari form di atas
                                fn (Builder $query, $value): Builder => $query->where('tahun_ajaran', 'like', "%{$value}%"),
                            );
                    }),
                // --- AKHIR BAGIAN YANG DIUBAH ---

                // Tables\Filters\TrashedFilter::make(), // Uncomment if you use Soft Deletes
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
                Tables\Grouping\Group::make('tahun_ajaran')
                    ->label('Tahun Ajaran')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            //
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListDimSemesters::route('/'),
            'create' => Pages\CreateDimSemester::route('/create'),
            'edit' => Pages\EditDimSemester::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count();
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::count() > 5 ? 'primary' : 'warning';
    }
}