// === ENTERPRISE PLATFORM CONTROLLER ===

class EnterprisePlatform {
    constructor() {
        this.currentSection = 'overview';
        this.productIntelligence = new ProductIntelligence();
        this.treasuryManagement = new TreasuryManagement();
        this.init();
    }
    
    init() {
        // Set initialization time
        const initTimeElement = document.getElementById('init-time');
        if (initTimeElement) {
            initTimeElement.textContent = new Date().toLocaleTimeString();
        }
        
        // Initialize modules
        this.productIntelligence.init();
        this.treasuryManagement.init();
        
        console.log('Enterprise NetSuite BI Platform initialized');
    }
    
    showSection(sectionId) {
        // Hide all sections
        document.querySelectorAll('.phase-section').forEach(section => {
            section.classList.remove('active');
        });
        
        // Show platform overview by default
        const overview = document.getElementById('overview');
        if (sectionId === 'overview') {
            overview.style.display = 'block';
        } else {
            overview.style.display = 'none';
            const targetSection = document.getElementById(sectionId + '-section');
            if (targetSection) {
                targetSection.classList.add('active');
            }
        }
        
        // Update navigation
        document.querySelectorAll('.nav-links a').forEach(link => {
            link.classList.remove('active');
        });
        
        const activeLink = document.querySelector(`a[href="#${sectionId}"]`);
        if (activeLink) {
            activeLink.classList.add('active');
        }
        
        this.currentSection = sectionId;
    }
}

// === PRODUCT INTELLIGENCE MODULE ===

class ProductIntelligence {
    constructor() {
        this.data = {
            itemCost: null,
            salesData: null,
            customerData: null,
            poData: null
        };
        this.charts = {};
    }
    
    init() {
        this.setupEventListeners();
        this.generateDemoCharts();
    }
    
    setupEventListeners() {
        // File upload handlers would go here
        // For demo purposes, we'll skip the file handling
        console.log('Product Intelligence event listeners initialized');
    }
    
    generateDemoCharts() {
        setTimeout(() => {
            this.createProfitChart();
            this.createPerformanceChart();
        }, 500);
    }
    
    createProfitChart() {
        const ctx = document.getElementById('product-profit-chart');
        if (!ctx) return;
        
        if (this.charts.profit) {
            this.charts.profit.destroy();
        }
        
        this.charts.profit = new Chart(ctx.getContext('2d'), {
            type: 'doughnut',
            data: {
                labels: ['Profit Generators', 'Loss Makers', 'Break Even'],
                datasets: [{
                    data: [75, 15, 10],
                    backgroundColor: ['#27ae60', '#e74c3c', '#f39c12'],
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'bottom' }
                }
            }
        });
    }
    
    createPerformanceChart() {
        const ctx = document.getElementById('product-performance-chart');
        if (!ctx) return;
        
        if (this.charts.performance) {
            this.charts.performance.destroy();
        }
        
        this.charts.performance = new Chart(ctx.getContext('2d'), {
            type: 'bar',
            data: {
                labels: ['Q1', 'Q2', 'Q3', 'Q4'],
                datasets: [{
                    label: 'Revenue ($M)',
                    data: [3.2, 4.1, 3.8, 4.3],
                    backgroundColor: '#3498db'
                }, {
                    label: 'Profit ($M)',
                    data: [1.0, 1.3, 1.2, 1.4],
                    backgroundColor: '#27ae60'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' }
                },
                scales: {
                    y: { beginAtZero: true }
                }
            }
        });
    }
    
    static generateAnalysis() {
        console.log('Generating product intelligence analysis...');
        alert('Product Intelligence Analysis Generated!\n\n• 367 items losing money identified\n• $5.4M total profit analyzed\n• Top 5 profit generators highlighted\n• Optimization recommendations ready');
    }
    
    static optimizePortfolio() {
        console.log('Optimizing product portfolio...');
        alert('Product Portfolio Optimization Complete!\n\n• Recommend discontinuing 67 loss-making items\n• Focus investment on top 20 profit generators\n• Potential annual savings: $2.1M\n• Revenue improvement: $1.8M');
    }
    
    static exportReport() {
        console.log('Exporting product intelligence report...');
        alert('Product Intelligence Report Exported!\n\nReport includes:\n• Profitability analysis by product\n• Sales performance trends\n• Customer intelligence insights\n• Strategic recommendations');
    }
}

// === TREASURY MANAGEMENT MODULE ===

class TreasuryManagement {
    constructor() {
        this.data = {
            generalLedger: null,
            accountsReceivable: null,
            accountsPayable: null,
            historicalCashFlow: null,
            forecasts: {
                ninetyDay: null,
                sixMonth: null,
                twelveMonth: null
            }
        };
        this.charts = {};
    }
    
    init() {
        this.setupEventListeners();
        this.generateDemoCharts();
    }
    
    setupEventListeners() {
        // File upload handlers would go here
        // For demo purposes, we'll skip the file handling
        console.log('Treasury Management event listeners initialized');
    }
    
    addStatusMessage(text, type = 'processing', icon = '⏳') {
        const container = document.getElementById('status-messages');
        if (!container) return;
        
        const messageDiv = document.createElement('div');
        messageDiv.className = `status-message ${type}`;
        
        const currentTime = new Date().toLocaleTimeString();
        
        messageDiv.innerHTML = `
            ${type === 'processing' ? '<div class="loading-spinner"></div>' : `<span class="status-icon">${icon}</span>`}
            <span class="status-text">${text}</span>
            <span class="status-time">${currentTime}</span>
        `;
        
        container.appendChild(messageDiv);
        container.scrollTop = container.scrollHeight;
        
        return messageDiv;
    }
    
    generateDemoCharts() {
        setTimeout(() => {
            this.create90DayChart();
            this.create6MonthChart();
            this.createWorkingCapitalChart();
            this.createScenarioChart();
        }, 500);
    }
    
    create90DayChart() {
        const ctx = document.getElementById('forecast-90-chart');
        if (!ctx) return;
        
        if (this.charts.forecast90) {
            this.charts.forecast90.destroy();
        }
        
        const data = [];
        let amount = 850000;
        for (let day = 1; day <= 90; day++) {
            amount += (Math.random() - 0.5) * 20000;
            data.push(amount);
        }
        
        this.charts.forecast90 = new Chart(ctx.getContext('2d'), {
            type: 'line',
            data: {
                labels: Array.from({length: 90}, (_, i) => i + 1),
                datasets: [{
                    label: 'Cash Position',
                    data: data,
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: {
                        beginAtZero: false,
                        ticks: {
                            callback: value => this.formatCurrency(value)
                        }
                    },
                    x: { display: false }
                }
            }
        });
    }
    
    create6MonthChart() {
        const ctx = document.getElementById('forecast-6m-chart');
        if (!ctx) return;
        
        if (this.charts.forecast6m) {
            this.charts.forecast6m.destroy();
        }
        
        this.charts.forecast6m = new Chart(ctx.getContext('2d'), {
            type: 'bar',
            data: {
                labels: ['Month 1', 'Month 2', 'Month 3', 'Month 4', 'Month 5', 'Month 6'],
                datasets: [{
                    label: 'Cash Position',
                    data: [850000, 920000, 890000, 950000, 980000, 1020000],
                    backgroundColor: '#764ba2',
                    borderColor: '#667eea',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: {
                        beginAtZero: false,
                        ticks: {
                            callback: value => this.formatCurrency(value)
                        }
                    }
                }
            }
        });
    }
    
    createWorkingCapitalChart() {
        const ctx = document.getElementById('working-capital-chart');
        if (!ctx) return;
        
        if (this.charts.workingCapital) {
            this.charts.workingCapital.destroy();
        }
        
        this.charts.workingCapital = new Chart(ctx.getContext('2d'), {
            type: 'line',
            data: {
                labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
                datasets: [{
                    label: 'Current',
                    data: [850000, 820000, 890000, 910000, 875000, 920000],
                    borderColor: '#e74c3c',
                    borderWidth: 3,
                    fill: false
                }, {
                    label: 'Optimized',
                    data: [950000, 980000, 1020000, 1050000, 1030000, 1080000],
                    borderColor: '#27ae60',
                    borderWidth: 3,
                    fill: false
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: true, position: 'top' } },
                scales: {
                    y: {
                        ticks: {
                            callback: value => this.formatCurrency(value)
                        }
                    }
                }
            }
        });
    }
    
    createScenarioChart() {
        const ctx = document.getElementById('scenario-chart');
        if (!ctx) return;
        
        if (this.charts.scenario) {
            this.charts.scenario.destroy();
        }
        
        this.charts.scenario = new Chart(ctx.getContext('2d'), {
            type: 'line',
            data: {
                labels: ['M1', 'M2', 'M3', 'M4', 'M5', 'M6'],
                datasets: [{
                    label: 'Optimistic',
                    data: [650000, 700000, 770000, 850000, 940000, 1030000],
                    borderColor: '#27ae60',
                    borderWidth: 2,
                    fill: false
                }, {
                    label: 'Realistic',
                    data: [600000, 630000, 660000, 690000, 720000, 750000],
                    borderColor: '#3498db',
                    borderWidth: 3,
                    fill: false
                }, {
                    label: 'Pessimistic',
                    data: [550000, 540000, 530000, 520000, 510000, 500000],
                    borderColor: '#e74c3c',
                    borderWidth: 2,
                    fill: false
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: true, position: 'top' } },
                scales: {
                    y: {
                        ticks: {
                            callback: value => this.formatCurrency(value)
                        }
                    }
                }
            }
        });
    }
    
    formatCurrency(value) {
        if (!isFinite(value) || value === null || value === undefined) return '$0';
        
        const absValue = Math.abs(value);
        const sign = value < 0 ? '-' : '';
        
        if (absValue >= 1000000) {
            return `${sign}$${(absValue / 1000000).toFixed(1)}M`;
        } else if (absValue >= 1000) {
            return `${sign}$${Math.round(absValue / 1000)}K`;
        } else {
            return `${sign}$${Math.round(absValue).toLocaleString()}`;
        }
    }
    
    static generateForecast() {
        const instance = window.platform.treasuryManagement;
        const statusMessage = instance.addStatusMessage('Generating comprehensive cash flow forecast', 'processing');
        
        setTimeout(() => {
            instance.addStatusMessage('90-day tactical forecast complete - cash position optimized', 'success', '📊');
            instance.addStatusMessage('6-month strategic projections generated with scenario analysis', 'success', '🎯');
            instance.addStatusMessage('Working capital optimization identified $350K annual opportunity', 'insight', '💰');
        }, 1500);
    }
    
    static runScenarioAnalysis() {
        const instance = window.platform.treasuryManagement;
        const statusMessage = instance.addStatusMessage('Running comprehensive scenario analysis', 'processing');
        
        setTimeout(() => {
            instance.addStatusMessage('Scenario analysis complete - all projections updated', 'success', '🎲');
            instance.addStatusMessage('Optimistic scenario: +37% upside potential identified', 'insight', '📈');
            instance.addStatusMessage('Pessimistic scenario: 4-month cash runway in worst case', 'insight', '⚠️');
        }, 2000);
    }
    
    static optimizeWorkingCapital() {
        const instance = window.platform.treasuryManagement;
        const statusMessage = instance.addStatusMessage('Analyzing working capital optimization opportunities', 'processing');
        
        setTimeout(() => {
            instance.addStatusMessage('Working capital optimization complete', 'success', '⚙️');
            instance.addStatusMessage('Recommendation: Accelerate customer payments with 2% early discount', 'insight', '💡');
            instance.addStatusMessage('Recommendation: Extend supplier terms from 30 to 45 days', 'insight', '💡');
            instance.addStatusMessage('Estimated annual improvement: $350K working capital optimization', 'success', '💰');
        }, 1500);
    }
    
    static generateExecutiveReport() {
        const instance = window.platform.treasuryManagement;
        const statusMessage = instance.addStatusMessage('Generating executive treasury report', 'processing');
        
        setTimeout(() => {
            instance.addStatusMessage('Executive treasury report generated successfully', 'success', '📄');
            instance.addStatusMessage('Report includes: Strategic forecasts, optimization recommendations, risk analysis', 'success', '📊');
        }, 1500);
    }
}

// === GLOBAL FUNCTIONS ===

function showOverview() {
    window.platform.showSection('overview');
}

function showProductIntelligence() {
    window.platform.showSection('product-intelligence');
}

function showTreasuryManagement() {
    window.platform.showSection('treasury-management');
}

function exportPlatformReport() {
    alert('Platform Report Exported!\n\nComprehensive enterprise report including:\n• Product Intelligence analysis ($8.0M+ opportunities)\n• Treasury Management forecasts\n• Working capital optimization\n• Strategic recommendations\n• Executive summary');
}

function exportDashboard() {
    alert('Dashboard Exported!\n\nProfessional PDF export includes:\n• All charts and visualizations\n• Key performance metrics\n• Business intelligence insights\n• Print-ready formatting');
}

// === INITIALIZATION ===

document.addEventListener('DOMContentLoaded', function() {
    window.platform = new EnterprisePlatform();
    
    // Export functions for global access
    window.ProductIntelligence = ProductIntelligence;
    window.TreasuryManagement = TreasuryManagement;
    
    console.log('🚀 Enterprise NetSuite BI Platform fully initialized');
});
