/**
 * Table Utilities - Reusable sorting and filtering for tables
 * Dynamic.IQ-COREx
 */

(function() {
    'use strict';

    window.TableUtils = {
        /**
         * Initialize sortable table headers
         * @param {string} tableSelector - CSS selector for the table
         * @param {object} options - Configuration options
         */
        initSortable: function(tableSelector, options = {}) {
            const table = document.querySelector(tableSelector);
            if (!table) return;

            const thead = table.querySelector('thead');
            const tbody = table.querySelector('tbody');
            if (!thead || !tbody) return;

            const headers = thead.querySelectorAll('th');
            let currentSort = { column: null, direction: 'asc' };

            headers.forEach((header, index) => {
                if (header.dataset.sortable === 'false') return;
                
                const sortType = header.dataset.sortType || 'string';
                
                header.style.cursor = 'pointer';
                header.style.userSelect = 'none';
                
                const icon = document.createElement('i');
                icon.className = 'bi bi-arrow-down-up ms-1 text-muted sort-icon';
                icon.style.fontSize = '0.8em';
                header.appendChild(icon);
                
                header.addEventListener('click', function() {
                    let direction = 'asc';
                    if (currentSort.column === index) {
                        direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
                    }
                    
                    currentSort = { column: index, direction: direction };
                    
                    headers.forEach(h => {
                        const sortIcon = h.querySelector('.sort-icon');
                        if (sortIcon) {
                            sortIcon.className = 'bi bi-arrow-down-up ms-1 text-muted sort-icon';
                        }
                    });
                    
                    icon.className = direction === 'asc' 
                        ? 'bi bi-arrow-up ms-1 text-primary sort-icon'
                        : 'bi bi-arrow-down ms-1 text-primary sort-icon';
                    
                    sortTable(tbody, index, direction, sortType);
                });
            });

            function parseNumericValue(value) {
                if (!value || value === '-' || value === 'N/A' || value === 'TBD') return 0;
                const cleaned = value.replace(/[^0-9.\-]/g, '');
                return parseFloat(cleaned) || 0;
            }

            function parseDate(value) {
                if (!value || value === '-' || value === 'N/A') return new Date(0);
                const date = new Date(value);
                return isNaN(date.getTime()) ? new Date(0) : date;
            }

            function sortTable(tbody, column, direction, sortType) {
                const rows = Array.from(tbody.querySelectorAll('tr'));
                
                rows.sort((a, b) => {
                    const cellA = a.cells[column];
                    const cellB = b.cells[column];
                    
                    if (!cellA || !cellB) return 0;
                    
                    let valueA = cellA.dataset.sortValue || cellA.textContent.trim();
                    let valueB = cellB.dataset.sortValue || cellB.textContent.trim();
                    
                    let comparison = 0;
                    
                    switch (sortType) {
                        case 'number':
                        case 'currency':
                            comparison = parseNumericValue(valueA) - parseNumericValue(valueB);
                            break;
                        case 'date':
                            comparison = parseDate(valueA) - parseDate(valueB);
                            break;
                        default:
                            comparison = valueA.toLowerCase().localeCompare(valueB.toLowerCase());
                    }
                    
                    return direction === 'asc' ? comparison : -comparison;
                });
                
                rows.forEach(row => tbody.appendChild(row));
            }
        },

        /**
         * Initialize client-side table filter
         * @param {string} tableSelector - CSS selector for the table
         * @param {string} inputSelector - CSS selector for the search input
         * @param {object} options - Configuration options
         */
        initFilter: function(tableSelector, inputSelector, options = {}) {
            const table = document.querySelector(tableSelector);
            const input = document.querySelector(inputSelector);
            if (!table || !input) return;

            const tbody = table.querySelector('tbody');
            if (!tbody) return;

            const excludeColumns = options.excludeColumns || [];
            const debounceMs = options.debounce || 300;
            let debounceTimer;

            input.addEventListener('input', function() {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(() => {
                    filterTable(this.value.toLowerCase().trim());
                }, debounceMs);
            });

            function filterTable(searchTerm) {
                const rows = tbody.querySelectorAll('tr');
                let visibleCount = 0;
                
                rows.forEach(row => {
                    if (searchTerm === '') {
                        row.style.display = '';
                        visibleCount++;
                        return;
                    }
                    
                    let matches = false;
                    const cells = row.querySelectorAll('td');
                    
                    cells.forEach((cell, index) => {
                        if (excludeColumns.includes(index)) return;
                        
                        const text = (cell.dataset.filterValue || cell.textContent).toLowerCase();
                        if (text.includes(searchTerm)) {
                            matches = true;
                        }
                    });
                    
                    row.style.display = matches ? '' : 'none';
                    if (matches) visibleCount++;
                });

                if (options.onFilter) {
                    options.onFilter(visibleCount, rows.length);
                }
            }
        },

        /**
         * Initialize both sorting and filtering
         * @param {string} tableSelector - CSS selector for the table
         * @param {string} filterInputSelector - CSS selector for the filter input (optional)
         * @param {object} options - Configuration options
         */
        init: function(tableSelector, filterInputSelector, options = {}) {
            this.initSortable(tableSelector, options);
            if (filterInputSelector) {
                this.initFilter(tableSelector, filterInputSelector, options);
            }
        },

        /**
         * Add a quick filter input above a table
         * @param {string} tableSelector - CSS selector for the table
         * @param {string} placeholder - Placeholder text for the input
         * @returns {HTMLElement} The created input element
         */
        addQuickFilter: function(tableSelector, placeholder = 'Quick search...') {
            const table = document.querySelector(tableSelector);
            if (!table) return null;

            const container = document.createElement('div');
            container.className = 'mb-3';
            container.innerHTML = `
                <div class="input-group input-group-sm" style="max-width: 300px;">
                    <span class="input-group-text"><i class="bi bi-search"></i></span>
                    <input type="text" class="form-control table-quick-filter" placeholder="${placeholder}">
                    <button class="btn btn-outline-secondary quick-filter-clear" type="button" style="display: none;">
                        <i class="bi bi-x"></i>
                    </button>
                </div>
            `;

            table.parentNode.insertBefore(container, table);

            const input = container.querySelector('.table-quick-filter');
            const clearBtn = container.querySelector('.quick-filter-clear');
            
            input.addEventListener('input', function() {
                clearBtn.style.display = this.value ? 'block' : 'none';
            });

            clearBtn.addEventListener('click', function() {
                input.value = '';
                input.dispatchEvent(new Event('input'));
                clearBtn.style.display = 'none';
            });

            return input;
        }
    };
})();
