from django.shortcuts import render, redirect

from .api_service import get_and_map_data, save_data_to_csv, save_metadata, read_from_csv, read_aggregated_data, \
    read_csv_header
from .models import ImportedFiles


def index(request):
    imported_files = ImportedFiles.objects.order_by('-created_at')
    context = {'imported_files': imported_files}
    return render(request, 'index.html', context)


def fetch_data(request):
    data = get_and_map_data()
    file_name = save_data_to_csv(data)
    save_metadata(file_name)
    return redirect('index')


def browse_data(request, file_id, limit):
    file = ImportedFiles.objects.get(id=file_id)
    columns = read_csv_header(file.file_name)
    people = read_from_csv(file.file_name, limit)
    context = {'people': people, 'file_name': file.file_name, 'columns': columns, 'file_id': file.id, 'limit': limit}
    return render(request, 'browse_data.html', context)


def browse_aggregate_data(request, file_id, selected_columns):
    file = ImportedFiles.objects.get(id=file_id)
    columns = read_csv_header(file.file_name)
    filter_columns = []
    for column in columns:
        if column in selected_columns:
            filter_columns.append(column)
    people = read_aggregated_data(file.file_name, filter_columns)
    context = {'people': people, 'file_id': file.id, 'file_name': file.file_name, 'columns': columns,
               'selected_columns': selected_columns}
    return render(request, 'browse_aggregate_data.html', context)
