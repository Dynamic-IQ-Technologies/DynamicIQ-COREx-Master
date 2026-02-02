const SystemGuidedMode = {
    isActive: false,
    currentStep: 0,
    steps: [],
    formEl: null,
    transactionType: '',
    userLevel: 'standard',
    
    init: function() {
        this.createModeToggle();
        this.detectUserLevel();
        this.bindGlobalEvents();
    },
    
    createModeToggle: function() {
        if (document.getElementById('guided-mode-toggle')) return;
        
        const form = document.querySelector('form');
        if (!form) return;
        
        // Create wrapper to isolate toggle from grid layouts
        const wrapper = document.createElement('div');
        wrapper.className = 'guided-mode-toggle-wrapper';
        
        const toggle = document.createElement('div');
        toggle.id = 'guided-mode-toggle';
        toggle.className = 'guided-mode-toggle';
        toggle.innerHTML = `
            <div class="gm-toggle-switch">
                <input type="checkbox" id="gm-switch" onchange="SystemGuidedMode.toggleMode(this.checked)">
                <label for="gm-switch">
                    <i class="bi bi-lightbulb"></i>
                    <span>System-Guided Mode</span>
                </label>
            </div>
        `;
        
        wrapper.appendChild(toggle);
        
        const mainContent = document.querySelector('.main-content');
        const card = mainContent?.querySelector('.card') || form.closest('.card');
        const container = document.querySelector('.container-fluid, .container');
        
        // Insert before the first .row or card that contains dashboard cards
        const dashboardRow = mainContent?.querySelector('.row');
        
        if (dashboardRow && dashboardRow.querySelector('.card, [class*="col-"]')) {
            dashboardRow.parentNode.insertBefore(wrapper, dashboardRow);
        } else if (card) {
            card.parentNode.insertBefore(wrapper, card);
        } else if (container) {
            container.insertBefore(wrapper, container.firstChild);
        } else if (form.parentNode) {
            form.parentNode.insertBefore(wrapper, form);
        }
    },
    
    detectUserLevel: function() {
        const loginCount = parseInt(localStorage.getItem('corex_login_count') || '0');
        if (loginCount > 50) {
            this.userLevel = 'advanced';
        } else if (loginCount > 10) {
            this.userLevel = 'intermediate';
        } else {
            this.userLevel = 'new';
        }
        localStorage.setItem('corex_login_count', (loginCount + 1).toString());
    },
    
    toggleMode: function(active) {
        this.isActive = active;
        document.body.classList.toggle('guided-mode-active', active);
        
        if (active) {
            this.activateGuidedMode();
        } else {
            this.deactivateGuidedMode();
        }
    },
    
    activateGuidedMode: function() {
        this.detectTransactionType();
        this.findFormFields();
        this.createProgressBar();
        this.createGuidancePanel();
        
        if (this.steps.length > 0) {
            this.showStep(0);
            this.showWelcomeMessage();
        }
    },
    
    deactivateGuidedMode: function() {
        document.querySelectorAll('.gm-highlight, .gm-field-guidance, .gm-progress-bar, .gm-guidance-panel, .gm-field-highlight, .gm-validation-feedback').forEach(el => el.remove());
        document.querySelectorAll('.gm-current-field').forEach(el => el.classList.remove('gm-current-field'));
        document.querySelectorAll('.gm-input-highlight').forEach(el => el.classList.remove('gm-input-highlight'));
        document.querySelectorAll('.gm-locked-field').forEach(el => {
            el.classList.remove('gm-locked-field');
        });
        document.querySelectorAll('[data-gm-locked]').forEach(el => {
            el.removeAttribute('data-gm-locked');
            if (el.hasAttribute('data-original-tabindex')) {
                el.tabIndex = parseInt(el.getAttribute('data-original-tabindex'));
                el.removeAttribute('data-original-tabindex');
            }
        });
        document.querySelectorAll('.is-valid, .is-invalid').forEach(el => {
            el.classList.remove('is-valid', 'is-invalid');
        });
        this.currentStep = 0;
        this.steps = [];
    },
    
    detectTransactionType: function() {
        const path = window.location.pathname;
        const pageTitle = document.querySelector('h2, h3, .card-title')?.textContent || '';
        
        if (path.includes('/workorders/') || pageTitle.toLowerCase().includes('work order')) {
            this.transactionType = 'Work Order';
        } else if (path.includes('/salesorders/') || pageTitle.toLowerCase().includes('sales order')) {
            this.transactionType = 'Sales Order';
        } else if (path.includes('/purchaseorders/') || pageTitle.toLowerCase().includes('purchase order')) {
            this.transactionType = 'Purchase Order';
        } else if (path.includes('/inventory/') || pageTitle.toLowerCase().includes('inventory')) {
            this.transactionType = 'Inventory Transaction';
        } else if (path.includes('/exchanges/') || pageTitle.toLowerCase().includes('exchange')) {
            this.transactionType = 'Exchange Transaction';
        } else if (path.includes('/shipping/') || pageTitle.toLowerCase().includes('ship')) {
            this.transactionType = 'Shipping Document';
        } else if (path.includes('/invoices/') || pageTitle.toLowerCase().includes('invoice')) {
            this.transactionType = 'Invoice';
        } else {
            this.transactionType = 'Transaction';
        }
    },
    
    findFormFields: function() {
        this.steps = [];
        this.formEl = document.querySelector('form');
        if (!this.formEl) return;
        
        const fieldConfigs = this.getFieldConfigurations();
        const formGroups = this.formEl.querySelectorAll('.form-group, .mb-3, .col-md-6, .col-md-4, .col-md-3, .col-12');
        
        formGroups.forEach((group, index) => {
            const input = group.querySelector('input, select, textarea');
            const label = group.querySelector('label');
            
            if (input && label && !input.type?.match(/hidden|submit|button/)) {
                const fieldName = input.name || input.id || '';
                const config = fieldConfigs[fieldName] || {};
                
                this.steps.push({
                    element: group,
                    input: input,
                    label: label.textContent.trim(),
                    name: fieldName,
                    required: input.required || input.hasAttribute('required') || config.required,
                    guidance: config.guidance || this.generateDefaultGuidance(label.textContent, input),
                    compliance: config.compliance || null,
                    validation: config.validation || null,
                    impact: config.impact || null,
                    suggestions: config.suggestions || null,
                    order: config.order || index
                });
            }
        });
        
        this.steps.sort((a, b) => a.order - b.order);
        this.steps = this.steps.filter(s => s.required || this.userLevel === 'new');
    },
    
    getFieldConfigurations: function() {
        return {
            'customer_id': {
                order: 1,
                required: true,
                guidance: 'Select the customer for this transaction. This establishes the billing relationship and determines applicable pricing.',
                compliance: 'Required for traceability per ISO 9001 and AS9100 standards.',
                impact: 'Affects: Pricing, payment terms, shipping address, and invoice generation.'
            },
            'product_id': {
                order: 2,
                required: true,
                guidance: 'Select the product or part number. The system will auto-populate pricing and inventory data.',
                compliance: 'Part traceability is mandatory for aerospace and defense contracts.',
                impact: 'Links to: BOM, inventory allocation, cost tracking, and quality records.'
            },
            'quantity': {
                order: 3,
                required: true,
                guidance: 'Enter the quantity. The system will check available inventory and flag shortages.',
                validation: (val) => parseFloat(val) > 0 ? null : 'Quantity must be greater than zero.',
                impact: 'Affects: Inventory levels, cost calculations, and material planning.'
            },
            'unit_price': {
                order: 4,
                required: true,
                guidance: 'Enter the unit price. Historical pricing data is available for reference.',
                validation: (val) => parseFloat(val) >= 0 ? null : 'Price cannot be negative.',
                impact: 'Affects: Revenue recognition, margin calculations, and commission tracking.'
            },
            'po_number': {
                order: 5,
                required: true,
                guidance: 'Enter the customer PO number for reference and traceability.',
                compliance: 'Required for contract compliance and audit trail.',
            },
            'required_date': {
                order: 6,
                required: true,
                guidance: 'Set the required delivery date. The system will validate against production capacity.',
                impact: 'Affects: Production scheduling, material ordering, and on-time delivery metrics.'
            },
            'serial_number': {
                order: 7,
                required: false,
                guidance: 'Enter serial number for serialized parts. Required for traceability.',
                compliance: 'Mandatory for serialized components per FAA/EASA requirements.'
            },
            'notes': {
                order: 99,
                required: false,
                guidance: 'Add any special instructions or notes for this transaction.'
            }
        };
    },
    
    generateDefaultGuidance: function(label, input) {
        const labelLower = label.toLowerCase();
        
        if (labelLower.includes('date')) {
            return 'Select the appropriate date. Ensure it aligns with operational schedules.';
        } else if (labelLower.includes('quantity') || labelLower.includes('qty')) {
            return 'Enter the quantity. The system validates against available inventory.';
        } else if (labelLower.includes('price') || labelLower.includes('cost')) {
            return 'Enter the monetary value. Historical data available for reference.';
        } else if (labelLower.includes('customer')) {
            return 'Select the customer associated with this transaction.';
        } else if (labelLower.includes('product') || labelLower.includes('part')) {
            return 'Select the product or part number from the catalog.';
        } else if (input.tagName === 'SELECT') {
            return 'Choose the appropriate option from the dropdown.';
        } else if (input.type === 'textarea') {
            return 'Provide detailed information as needed.';
        } else {
            return 'Complete this field to continue.';
        }
    },
    
    createProgressBar: function() {
        const existing = document.querySelector('.gm-progress-bar');
        if (existing) existing.remove();
        
        const progressBar = document.createElement('div');
        progressBar.className = 'gm-progress-bar';
        progressBar.innerHTML = `
            <div class="gm-progress-header">
                <span class="gm-progress-title"><i class="bi bi-lightbulb-fill"></i> ${this.transactionType} - System Guided</span>
                <span class="gm-progress-count">Step <span id="gm-current-step">1</span> of <span id="gm-total-steps">${this.steps.length}</span></span>
            </div>
            <div class="gm-progress-track">
                <div class="gm-progress-fill" id="gm-progress-fill"></div>
            </div>
            <div class="gm-step-indicators" id="gm-step-indicators"></div>
        `;
        
        const form = document.querySelector('form');
        if (form) {
            form.parentNode.insertBefore(progressBar, form);
        }
        
        this.updateProgressIndicators();
    },
    
    updateProgressIndicators: function() {
        const container = document.getElementById('gm-step-indicators');
        if (!container) return;
        
        container.innerHTML = this.steps.slice(0, 8).map((step, i) => `
            <div class="gm-step-dot ${i < this.currentStep ? 'completed' : ''} ${i === this.currentStep ? 'active' : ''}" 
                 title="${step.label}" onclick="SystemGuidedMode.goToStep(${i})">
                ${i < this.currentStep ? '<i class="bi bi-check"></i>' : (i + 1)}
            </div>
        `).join('');
        
        if (this.steps.length > 8) {
            container.innerHTML += `<span class="gm-more-steps">+${this.steps.length - 8} more</span>`;
        }
    },
    
    createGuidancePanel: function() {
        const existing = document.querySelector('.gm-guidance-panel');
        if (existing) existing.remove();
        
        const panel = document.createElement('div');
        panel.className = 'gm-guidance-panel';
        panel.id = 'gm-guidance-panel';
        panel.innerHTML = `
            <div class="gm-panel-header">
                <i class="bi bi-info-circle-fill"></i>
                <span>Field Guidance</span>
            </div>
            <div class="gm-panel-body">
                <div class="gm-field-name" id="gm-field-name"></div>
                <div class="gm-field-guidance" id="gm-field-guidance-text"></div>
                <div class="gm-compliance-note" id="gm-compliance-note"></div>
                <div class="gm-impact-note" id="gm-impact-note"></div>
                <div class="gm-suggestions" id="gm-suggestions"></div>
            </div>
            <div class="gm-panel-actions">
                <button type="button" class="btn btn-outline-secondary btn-sm" onclick="SystemGuidedMode.previousStep()">
                    <i class="bi bi-arrow-left"></i> Previous
                </button>
                <button type="button" class="btn btn-primary btn-sm" onclick="SystemGuidedMode.nextStep()">
                    Next <i class="bi bi-arrow-right"></i>
                </button>
            </div>
        `;
        
        document.body.appendChild(panel);
    },
    
    showStep: function(stepIndex) {
        if (stepIndex < 0 || stepIndex >= this.steps.length) return;
        
        document.querySelectorAll('.gm-current-field').forEach(el => el.classList.remove('gm-current-field'));
        document.querySelectorAll('.gm-field-highlight').forEach(el => el.remove());
        document.querySelectorAll('.gm-input-highlight').forEach(el => el.classList.remove('gm-input-highlight'));
        
        this.currentStep = stepIndex;
        const step = this.steps[stepIndex];
        
        this.lockNonCurrentFields(stepIndex);
        
        step.element.classList.add('gm-current-field');
        step.element.scrollIntoView({ behavior: 'smooth', block: 'center' });
        
        // Apply highlight directly to input instead of overlay
        step.input.classList.add('gm-input-highlight');
        
        setTimeout(() => step.input.focus(), 300);
        
        this.updateGuidancePanel(step);
        this.updateProgress();
        this.bindFieldValidation(step);
    },
    
    lockNonCurrentFields: function(currentIndex) {
        this.steps.forEach((step, i) => {
            if (i > currentIndex) {
                step.element.classList.add('gm-locked-field');
                step.input.setAttribute('data-gm-locked', 'true');
                if (!step.input.hasAttribute('data-original-tabindex')) {
                    step.input.setAttribute('data-original-tabindex', step.input.tabIndex || '0');
                }
                step.input.tabIndex = -1;
            } else {
                step.element.classList.remove('gm-locked-field');
                step.input.removeAttribute('data-gm-locked');
                if (step.input.hasAttribute('data-original-tabindex')) {
                    step.input.tabIndex = parseInt(step.input.getAttribute('data-original-tabindex'));
                }
            }
        });
    },
    
    updateGuidancePanel: function(step) {
        const panel = document.getElementById('gm-guidance-panel');
        if (!panel) return;
        
        document.getElementById('gm-field-name').innerHTML = `
            <strong>${step.label}</strong>
            ${step.required ? '<span class="badge bg-danger ms-2">Required</span>' : '<span class="badge bg-secondary ms-2">Optional</span>'}
        `;
        
        document.getElementById('gm-field-guidance-text').textContent = step.guidance;
        
        const complianceEl = document.getElementById('gm-compliance-note');
        if (step.compliance) {
            complianceEl.innerHTML = `<i class="bi bi-shield-check"></i> ${step.compliance}`;
            complianceEl.style.display = 'block';
        } else {
            complianceEl.style.display = 'none';
        }
        
        const impactEl = document.getElementById('gm-impact-note');
        if (step.impact) {
            impactEl.innerHTML = `<i class="bi bi-diagram-3"></i> ${step.impact}`;
            impactEl.style.display = 'block';
        } else {
            impactEl.style.display = 'none';
        }
        
        const suggestionsEl = document.getElementById('gm-suggestions');
        if (step.suggestions && step.suggestions.length > 0) {
            suggestionsEl.innerHTML = `
                <div class="gm-suggestion-title">Suggested values:</div>
                ${step.suggestions.map(s => `<button type="button" class="gm-suggestion-btn" onclick="SystemGuidedMode.applySuggestion('${s}')">${s}</button>`).join('')}
            `;
            suggestionsEl.style.display = 'block';
        } else {
            suggestionsEl.style.display = 'none';
        }
    },
    
    updateProgress: function() {
        const fill = document.getElementById('gm-progress-fill');
        const currentEl = document.getElementById('gm-current-step');
        
        if (fill) {
            const progress = ((this.currentStep + 1) / this.steps.length) * 100;
            fill.style.width = progress + '%';
        }
        
        if (currentEl) {
            currentEl.textContent = this.currentStep + 1;
        }
        
        this.updateProgressIndicators();
    },
    
    bindFieldValidation: function(step) {
        step.input.removeEventListener('blur', step._validateHandler);
        step.input.removeEventListener('change', step._validateHandler);
        
        step._validateHandler = () => this.validateField(step);
        step.input.addEventListener('blur', step._validateHandler);
        step.input.addEventListener('change', step._validateHandler);
    },
    
    validateField: function(step) {
        const value = step.input.value;
        let error = null;
        
        if (step.required && !value.trim()) {
            error = 'This field is required.';
        } else if (step.validation && value) {
            error = step.validation(value);
        }
        
        // Remove ALL existing feedback elements for this step (not just the first one)
        step.element.querySelectorAll('.gm-validation-feedback').forEach(el => el.remove());
        
        if (error) {
            const feedback = document.createElement('div');
            feedback.className = 'gm-validation-feedback error';
            feedback.innerHTML = `<i class="bi bi-exclamation-circle"></i> ${error}`;
            step.element.appendChild(feedback);
            step.input.classList.add('is-invalid');
            return false;
        } else if (value.trim()) {
            step.input.classList.remove('is-invalid');
            step.input.classList.add('is-valid');
            const feedback = document.createElement('div');
            feedback.className = 'gm-validation-feedback success';
            feedback.innerHTML = `<i class="bi bi-check-circle"></i> Valid`;
            step.element.appendChild(feedback);
            return true;
        }
        
        return true;
    },
    
    nextStep: function() {
        const currentStepData = this.steps[this.currentStep];
        if (!this.validateField(currentStepData)) {
            this.showWarning('Please complete this field correctly before proceeding.');
            return;
        }
        
        if (this.currentStep < this.steps.length - 1) {
            this.showStep(this.currentStep + 1);
        } else {
            this.showCompletionSummary();
        }
    },
    
    previousStep: function() {
        if (this.currentStep > 0) {
            this.showStep(this.currentStep - 1);
        }
    },
    
    goToStep: function(index) {
        if (index <= this.currentStep || this.validateAllUpTo(index)) {
            this.showStep(index);
        }
    },
    
    validateAllUpTo: function(index) {
        for (let i = 0; i < index; i++) {
            if (!this.validateField(this.steps[i])) {
                this.showStep(i);
                this.showWarning('Please complete all previous fields first.');
                return false;
            }
        }
        return true;
    },
    
    applySuggestion: function(value) {
        const step = this.steps[this.currentStep];
        if (step && step.input) {
            step.input.value = value;
            step.input.dispatchEvent(new Event('change', { bubbles: true }));
            this.validateField(step);
        }
    },
    
    showWelcomeMessage: function() {
        const levelMessages = {
            'new': 'Welcome! I\'ll guide you through each field step-by-step. Follow the highlighted sections.',
            'intermediate': 'Guided Mode active. Required fields are highlighted. Navigate with Next/Previous.',
            'advanced': 'Guided Mode on. Condensed guidance available.'
        };
        
        this.showNotification(levelMessages[this.userLevel] || levelMessages['intermediate'], 'info');
    },
    
    showWarning: function(message) {
        this.showNotification(message, 'warning');
    },
    
    showNotification: function(message, type) {
        const existing = document.querySelector('.gm-notification');
        if (existing) existing.remove();
        
        const notification = document.createElement('div');
        notification.className = `gm-notification gm-notification-${type}`;
        notification.innerHTML = `
            <i class="bi bi-${type === 'warning' ? 'exclamation-triangle' : 'info-circle'}"></i>
            <span>${message}</span>
            <button type="button" onclick="this.parentElement.remove()">&times;</button>
        `;
        document.body.appendChild(notification);
        
        setTimeout(() => notification.remove(), 5000);
    },
    
    showCompletionSummary: function() {
        const requiredSteps = this.steps.filter(s => s.required);
        const completedRequired = requiredSteps.filter(s => s.input.value.trim());
        const allRequiredComplete = completedRequired.length === requiredSteps.length;
        
        const completedFields = this.steps.filter(s => s.input.value.trim());
        const emptyOptional = this.steps.filter(s => !s.required && !s.input.value.trim());
        
        const modal = document.createElement('div');
        modal.className = 'gm-completion-modal';
        modal.innerHTML = `
            <div class="gm-completion-content">
                <div class="gm-completion-header" style="background: ${allRequiredComplete ? 'linear-gradient(135deg, #10b981 0%, #059669 100%)' : 'linear-gradient(135deg, #f59e0b 0%, #d97706 100%)'}">
                    <i class="bi bi-${allRequiredComplete ? 'check-circle-fill' : 'exclamation-triangle-fill'}"></i>
                    <h4>${allRequiredComplete ? 'Transaction Ready for Submission' : 'Missing Required Fields'}</h4>
                </div>
                <div class="gm-completion-body">
                    <p><strong>${this.transactionType}</strong> - ${completedRequired.length} of ${requiredSteps.length} required fields completed.</p>
                    
                    ${!allRequiredComplete ? `
                    <div class="gm-summary-section" style="background: #fef2f2; border: 1px solid #fecaca;">
                        <h6 style="color: #dc2626;"><i class="bi bi-exclamation-circle"></i> Missing Required:</h6>
                        <ul>
                            ${requiredSteps.filter(s => !s.input.value.trim()).map(s => `<li style="color: #dc2626;">${s.label}</li>`).join('')}
                        </ul>
                    </div>
                    ` : ''}
                    
                    <div class="gm-summary-section">
                        <h6><i class="bi bi-list-check"></i> Completed Fields (${completedFields.length}):</h6>
                        <ul>
                            ${completedFields.map(s => `<li><strong>${s.label}:</strong> ${s.input.value.substring(0, 50)}${s.input.value.length > 50 ? '...' : ''}</li>`).join('')}
                        </ul>
                    </div>
                    
                    ${emptyOptional.length > 0 ? `
                    <div class="gm-summary-section" style="background: #fef3c7;">
                        <h6 style="color: #92400e;"><i class="bi bi-info-circle"></i> Optional Fields Skipped (${emptyOptional.length}):</h6>
                        <ul style="color: #92400e;">
                            ${emptyOptional.map(s => `<li>${s.label}</li>`).join('')}
                        </ul>
                    </div>
                    ` : ''}
                    
                    <div class="gm-impact-summary">
                        <h6><i class="bi bi-diagram-3"></i> Downstream Impacts:</h6>
                        <ul>
                            <li>Inventory records will be updated</li>
                            <li>Audit trail entry will be created</li>
                            <li>Related modules will be synchronized</li>
                            <li>Traceability links established for compliance</li>
                        </ul>
                    </div>
                </div>
                <div class="gm-completion-actions">
                    <button type="button" class="btn btn-outline-secondary" onclick="this.closest('.gm-completion-modal').remove()">
                        Review Fields
                    </button>
                    ${allRequiredComplete ? `
                    <button type="button" class="btn btn-success" onclick="SystemGuidedMode.submitForm()">
                        <i class="bi bi-check-lg"></i> Confirm & Submit
                    </button>
                    ` : `
                    <button type="button" class="btn btn-warning" onclick="this.closest('.gm-completion-modal').remove(); SystemGuidedMode.goToFirstIncomplete()">
                        <i class="bi bi-arrow-right"></i> Complete Required Fields
                    </button>
                    `}
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    },
    
    goToFirstIncomplete: function() {
        const firstIncomplete = this.steps.findIndex(s => s.required && !s.input.value.trim());
        if (firstIncomplete >= 0) {
            this.showStep(firstIncomplete);
        }
    },
    
    submitForm: function() {
        document.querySelector('.gm-completion-modal')?.remove();
        if (this.formEl) {
            const submitBtn = this.formEl.querySelector('button[type="submit"], input[type="submit"]');
            if (submitBtn) {
                submitBtn.click();
            } else {
                this.formEl.submit();
            }
        }
    },
    
    bindGlobalEvents: function() {
        document.addEventListener('keydown', (e) => {
            if (!this.isActive) return;
            
            if (e.key === 'Tab' && !e.shiftKey) {
                e.preventDefault();
                this.nextStep();
            } else if (e.key === 'Tab' && e.shiftKey) {
                e.preventDefault();
                this.previousStep();
            }
        });
    }
};

document.addEventListener('DOMContentLoaded', function() {
    setTimeout(() => SystemGuidedMode.init(), 500);
});
