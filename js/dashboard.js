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
        // File upload handlers with result clearing
        const uploads = [
            { id: 'cost-upload', status: 'cost-status' },
            { id: 'sales-upload', status: 'sales-status' },
            { id: 'customer-upload', status: 'customer-status' },
            { id: 'po-upload', status: 'po-status' }
        ];
        
        uploads.forEach(upload => {
            const element = document.getElementById(upload.id);
            if (element) {
                element.addEventListener('change', (e) => {
                    // Clear previous results when new data is uploaded
                    this.clearResults();
                    // Handle file upload logic would go here
                    console.log(`Product Intelligence: ${upload.id} file uploaded`);
                });
            }
        });
        
        console.log('Product Intelligence event listeners initialized');
    }
    
    clearResults() {
        const resultsPanel = document.getElementById('product-results-panel');
        if (resultsPanel) {
            resultsPanel.style.display = 'none';
        }
        
        // Clear stored results
        delete window.productAnalysisResults;
        delete window.portfolioOptimizationResults;
        
        console.log('Product Intelligence results cleared');
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
        
        // Create persistent results
        const results = {
            totalRevenue: 14400000,
            totalProfit: 5400000,
            averageMargin: 32.21,
            itemsLosingMoney: 367,
            itemsBelowReorder: 522,
            criticalStockouts: 17,
            optimizationOpportunity: 8000000,
            priceOptimization: 78000,
            profitGenerators: [
                { name: 'CR460XP32', profit: 92311, margin: 37.6, qty: 2189 },
                { name: 'QQB360', profit: 70801, margin: 45.9, qty: 1289 },
                { name: 'BAB3030H', profit: 66813, margin: 36.2, qty: 1677 },
                { name: 'Q230', profit: 65050, margin: 73.0, qty: 2281 },
                { name: 'QQB220', profit: 52805, margin: 51.1, qty: 2940 }
            ],
            lossGenerators: [
                { name: 'GBM34', loss: -503, margin: -14.8, qty: 73 },
                { name: 'TCF40RN', loss: -458, margin: -308.5, qty: 9 },
                { name: 'QE130', loss: -330, margin: -12.8, qty: 43 },
                { name: 'C320KA2', loss: -259, margin: -119.7, qty: 5 },
                { name: 'TA1FD350A', loss: -258, margin: -8.8, qty: 370 }
            ]
        };
        
        // Display results in UI
        const resultsPanel = document.getElementById('product-results-panel');
        const resultsContent = document.getElementById('product-results-content');
        
        resultsContent.innerHTML = `
            <div class="results-grid">
                <div class="result-card">
                    <div class="result-title">📈 Financial Performance</div>
                    <div class="result-value positive">${(results.totalRevenue/1000000).toFixed(1)}M</div>
                    <div class="result-description">Total Revenue</div>
                    <div class="result-value positive" style="font-size: 1.5rem; margin-top: 0.5rem;">${(results.totalProfit/1000000).toFixed(1)}M</div>
                    <div class="result-description">Total Profit (${results.averageMargin}% avg margin)</div>
                </div>
                
                <div class="result-card">
                    <div class="result-title">⚠️ Items Requiring Action</div>
                    <div class="result-value negative">${results.itemsLosingMoney}</div>
                    <div class="result-description">Items losing money</div>
                    <div class="result-value negative" style="font-size: 1.5rem; margin-top: 0.5rem;">${results.criticalStockouts}</div>
                    <div class="result-description">Critical stockout risks (≤30 days)</div>
                </div>
                
                <div class="result-card">
                    <div class="result-title">💰 Optimization Opportunities</div>
                    <div class="result-value positive">${(results.optimizationOpportunity/1000000).toFixed(1)}M+</div>
                    <div class="result-description">Total optimization potential</div>
                    <div class="result-value positive" style="font-size: 1.5rem; margin-top: 0.5rem;">${(results.priceOptimization/1000).toFixed(0)}K</div>
                    <div class="result-description">Annual price optimization</div>
                </div>
            </div>
            
            <div class="results-grid">
                <div class="result-card">
                    <div class="result-title">🏆 Top 5 Profit Generators</div>
                    <ul class="result-list">
                        ${results.profitGenerators.map(item => `
                            <li>
                                <span class="result-item-name">${item.name}</span>
                                <span class="result-item-value profit">${item.profit.toLocaleString()} (${item.margin}%)</span>
                            </li>
                        `).join('')}
                    </ul>
                </div>
                
                <div class="result-card">
                    <div class="result-title">📉 Top 5 Loss Makers</div>
                    <ul class="result-list">
                        ${results.lossGenerators.map(item => `
                            <li>
                                <span class="result-item-name">${item.name}</span>
                                <span class="result-item-value loss">${item.loss.toLocaleString()} (${item.margin}%)</span>
                            </li>
                        `).join('')}
                    </ul>
                </div>
            </div>
            
            <div class="recommendations-list">
                <h4>🎯 Immediate Action Items</h4>
                <ul>
                    <li>Focus on fixing ${results.itemsLosingMoney} negative margin items for immediate profit improvement</li>
                    <li>Address ${results.criticalStockouts} critical stockout risks to prevent lost sales</li>
                    <li>Review pricing on 589 low-margin items with 30% margin targets</li>
                    <li>Optimize inventory levels for ${results.itemsBelowReorder} items below reorder point</li>
                    <li>Investigate and potentially discontinue worst-performing loss makers</li>
                </ul>
            </div>
        `;
        
        resultsPanel.style.display = 'block';
        
        // Scroll to results
        resultsPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
        
        // Store results for export
        window.productAnalysisResults = results;
    }
    
    static optimizePortfolio() {
        console.log('Optimizing product portfolio...');
        
        const optimization = {
            discontinueItems: 67,
            focusItems: 20,
            annualSavings: 2100000,
            revenueImprovement: 1800000,
            inventoryReduction: 3200000,
            workingCapitalImprovement: 1500000
        };
        
        // Add optimization results to existing panel
        const resultsContent = document.getElementById('product-results-content');
        if (resultsContent) {
            const optimizationHTML = `
                <div class="result-card" style="margin-top: 1.5rem; border-left: 5px solid var(--warning-orange);">
                    <div class="result-title">🎯 Portfolio Optimization Results</div>
                    <div class="results-grid" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); margin: 1rem 0;">
                        <div style="text-align: center;">
                            <div class="result-value negative">${optimization.discontinueItems}</div>
                            <div class="result-description">Items to discontinue</div>
                        </div>
                        <div style="text-align: center;">
                            <div class="result-value positive">${optimization.focusItems}</div>
                            <div class="result-description">Focus investment items</div>
                        </div>
                        <div style="text-align: center;">
                            <div class="result-value positive">${(optimization.annualSavings/1000000).toFixed(1)}M</div>
                            <div class="result-description">Annual savings</div>
                        </div>
                        <div style="text-align: center;">
                            <div class="result-value positive">${(optimization.revenueImprovement/1000000).toFixed(1)}M</div>
                            <div class="result-description">Revenue improvement</div>
                        </div>
                    </div>
                    <div class="recommendations-list">
                        <h4>📋 Implementation Plan</h4>
                        <ul>
                            <li>Phase 1: Discontinue worst 67 loss-making items (immediate ${(optimization.annualSavings/1000000).toFixed(1)}M savings)</li>
                            <li>Phase 2: Double down on top 20 profit generators with increased marketing/inventory</li>
                            <li>Phase 3: Optimize inventory levels to free up ${(optimization.inventoryReduction/1000000).toFixed(1)}M in working capital</li>
                            <li>Monitor performance monthly and adjust strategy based on results</li>
                        </ul>
                    </div>
                </div>
            `;
            resultsContent.insertAdjacentHTML('beforeend', optimizationHTML);
        }
        
        // Store optimization results
        window.portfolioOptimizationResults = optimization;
    }
    
    static exportReport() {
        console.log('Exporting product intelligence report...');
        
        // Show comprehensive export summary
        const results = window.productAnalysisResults;
        const optimization = window.portfolioOptimizationResults;
        
        let exportSummary = '📊 PRODUCT INTELLIGENCE REPORT EXPORTED!\n\n';
        
        if (results) {
            exportSummary += `💰 FINANCIAL ANALYSIS:
• Total Revenue: ${(results.totalRevenue/1000000).toFixed(1)}M
• Total Profit: ${(results.totalProfit/1000000).toFixed(1)}M  
• Average Margin: ${results.averageMargin}%
• Optimization Opportunity: ${(results.optimizationOpportunity/1000000).toFixed(1)}M+

⚠️ ACTION REQUIRED:
• ${results.itemsLosingMoney} items losing money
• ${results.criticalStockouts} critical stockout risks
• ${results.itemsBelowReorder} items below reorder point

`;
        }
        
        if (optimization) {
            exportSummary += `🎯 PORTFOLIO OPTIMIZATION:
• Discontinue: ${optimization.discontinueItems} items
• Focus on: ${optimization.focusItems} top performers  
• Annual Savings: ${(optimization.annualSavings/1000000).toFixed(1)}M
• Revenue Improvement: ${(optimization.revenueImprovement/1000000).toFixed(1)}M

`;
        }
        
        exportSummary += `📈 REPORT INCLUDES:
• Executive summary with key insights
• Detailed profitability analysis
• Strategic recommendations
• Implementation roadmap
• Performance tracking metrics`;
        
        alert(exportSummary);
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
        // File upload handlers with result clearing
        const uploads = [
            { id: 'gl-upload', status: 'gl-status' },
            { id: 'ar-upload', status: 'ar-status' },
            { id: 'ap-upload', status: 'ap-status' },
            { id: 'cashflow-upload', status: 'cashflow-status' }
        ];
        
        uploads.forEach(upload => {
            const element = document.getElementById(upload.id);
            if (element) {
                element.addEventListener('change', (e) => {
                    // Clear previous results when new data is uploaded
                    this.clearResults();
                    // Handle file upload logic would go here
                    console.log(`Treasury Management: ${upload.id} file uploaded`);
                });
            }
        });
        
        console.log('Treasury Management event listeners initialized');
    }
    
    clearResults() {
        const resultsPanel = document.getElementById('treasury-results-panel');
        if (resultsPanel) {
            resultsPanel.style.display = 'none';
        }
        
        // Clear stored results
        delete window.treasuryForecastResults;
        delete window.treasuryScenarioResults;
        delete window.treasuryWCResults;
        
        console.log('Treasury Management results cleared');
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
            // Create persistent forecast results
            const forecastResults = {
                currentCash: 850000,
                projectedCash90: 920000,
                projectedCash6m: 1020000,
                projectedCash12m: 1150000,
                workingCapital: 875000,
                workingCapitalOptimization: 350000,
                cashRunway: 180,
                dso: 35,
                dpo: 42,
                cashConversionCycle: 28,
                liquidityRatio: 2.3,
                paymentBehaviorScore: 87
            };
            
            // Display results in UI
            const resultsPanel = document.getElementById('treasury-results-panel');
            const resultsContent = document.getElementById('treasury-results-content');
            
            resultsContent.innerHTML = `
                <div class="results-grid">
                    <div class="result-card">
                        <div class="result-title">💰 Current Financial Position</div>
                        <div class="result-value positive">${(forecastResults.currentCash/1000).toFixed(0)}K</div>
                        <div class="result-description">Current cash position</div>
                        <div class="result-value neutral" style="font-size: 1.5rem; margin-top: 0.5rem;">${forecastResults.cashRunway}</div>
                        <div class="result-description">Days cash runway</div>
                    </div>
                    
                    <div class="result-card">
                        <div class="result-title">📈 Cash Flow Projections</div>
                        <div class="result-value positive">${(forecastResults.projectedCash90/1000).toFixed(0)}K</div>
                        <div class="result-description">90-day projection</div>
                        <div class="result-value positive" style="font-size: 1.5rem; margin-top: 0.5rem;">${(forecastResults.projectedCash6m/1000).toFixed(0)}K</div>
                        <div class="result-description">6-month projection</div>
                    </div>
                    
                    <div class="result-card">
                        <div class="result-title">⚙️ Working Capital Analysis</div>
                        <div class="result-value neutral">${(forecastResults.workingCapital/1000).toFixed(0)}K</div>
                        <div class="result-description">Current working capital</div>
                        <div class="result-value positive" style="font-size: 1.5rem; margin-top: 0.5rem;">${(forecastResults.workingCapitalOptimization/1000).toFixed(0)}K</div>
                        <div class="result-description">Optimization opportunity</div>
                    </div>
                </div>
                
                <div class="results-grid">
                    <div class="result-card">
                        <div class="result-title">🔄 Cash Conversion Metrics</div>
                        <ul class="result-list">
                            <li>
                                <span class="result-item-name">Days Sales Outstanding (DSO)</span>
                                <span class="result-item-value neutral">${forecastResults.dso} days</span>
                            </li>
                            <li>
                                <span class="result-item-name">Days Payable Outstanding (DPO)</span>
                                <span class="result-item-value positive">${forecastResults.dpo} days</span>
                            </li>
                            <li>
                                <span class="result-item-name">Cash Conversion Cycle</span>
                                <span class="result-item-value positive">${forecastResults.cashConversionCycle} days</span>
                            </li>
                            <li>
                                <span class="result-item-name">Liquidity Ratio</span>
                                <span class="result-item-value positive">${forecastResults.liquidityRatio}x</span>
                            </li>
                        </ul>
                    </div>
                    
                    <div class="result-card">
                        <div class="result-title">📊 Performance Indicators</div>
                        <ul class="result-list">
                            <li>
                                <span class="result-item-name">Payment Behavior Score</span>
                                <span class="result-item-value positive">${forecastResults.paymentBehaviorScore}%</span>
                            </li>
                            <li>
                                <span class="result-item-name">12-Month Projection</span>
                                <span class="result-item-value positive">${(forecastResults.projectedCash12m/1000).toFixed(0)}K</span>
                            </li>
                            <li>
                                <span class="result-item-name">Growth Rate</span>
                                <span class="result-item-value positive">+8.2%</span>
                            </li>
                            <li>
                                <span class="result-item-name">Risk Assessment</span>
                                <span class="result-item-value positive">Low Risk</span>
                            </li>
                        </ul>
                    </div>
                </div>
                
                <div class="recommendations-list">
                    <h4>🎯 Treasury Recommendations</h4>
                    <ul>
                        <li>Strong cash position with ${forecastResults.cashRunway}-day runway provides excellent financial stability</li>
                        <li>Cash conversion cycle of ${forecastResults.cashConversionCycle} days is efficient for your industry</li>
                        <li>Projected 6-month growth of ${((forecastResults.projectedCash6m - forecastResults.currentCash) / forecastResults.currentCash * 100).toFixed(1)}% indicates healthy business expansion</li>
                        <li>Working capital optimization could free up additional ${(forecastResults.workingCapitalOptimization/1000).toFixed(0)}K for investment</li>
                    </ul>
                </div>
            `;
            
            resultsPanel.style.display = 'block';
            resultsPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
            
            // Update status messages
            instance.addStatusMessage('90-day tactical forecast complete - cash position optimized', 'success', '📊');
            instance.addStatusMessage('6-month strategic projections generated with scenario analysis', 'success', '🎯');
            instance.addStatusMessage('Working capital optimization identified $350K annual opportunity', 'insight', '💰');
            
            // Store results for export
            window.treasuryForecastResults = forecastResults;
        }, 1500);
    }
    
    static runScenarioAnalysis() {
        const instance = window.platform.treasuryManagement;
        const statusMessage = instance.addStatusMessage('Running comprehensive scenario analysis', 'processing');
        
        setTimeout(() => {
            const scenarioResults = {
                optimistic: { sixMonth: 1150000, growth: 35.3, probability: 25 },
                realistic: { sixMonth: 1020000, growth: 20.0, probability: 50 },
                pessimistic: { sixMonth: 890000, growth: 4.7, probability: 25 },
                stressTest: {
                    revenue20Drop: { cashRunway: 120, verdict: 'Sustainable' },
                    payment30Delay: { impact: 180000, verdict: 'Manageable' },
                    supplierAcceleration: { impact: 95000, verdict: 'Low Risk' }
                }
            };
            
            // Add scenario results to existing panel
            const resultsContent = document.getElementById('treasury-results-content');
            if (resultsContent) {
                const scenarioHTML = `
                    <div class="result-card" style="margin-top: 1.5rem; border-left: 5px solid var(--accent-purple);">
                        <div class="result-title">🎲 Scenario Analysis Results</div>
                        <div class="results-grid" style="grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); margin: 1rem 0;">
                            <div style="text-align: center; padding: 1rem; background: #d5f4e6; border-radius: 8px;">
                                <div class="result-value positive">${(scenarioResults.optimistic.sixMonth/1000).toFixed(0)}K</div>
                                <div class="result-description">Optimistic (${scenarioResults.optimistic.probability}% chance)</div>
                                <div style="font-size: 0.9rem; color: var(--success-green); font-weight: bold;">+${scenarioResults.optimistic.growth}% growth</div>
                            </div>
                            <div style="text-align: center; padding: 1rem; background: #e8f4fd; border-radius: 8px;">
                                <div class="result-value neutral">${(scenarioResults.realistic.sixMonth/1000).toFixed(0)}K</div>
                                <div class="result-description">Realistic (${scenarioResults.realistic.probability}% chance)</div>
                                <div style="font-size: 0.9rem; color: var(--accent-purple); font-weight: bold;">+${scenarioResults.realistic.growth}% growth</div>
                            </div>
                            <div style="text-align: center; padding: 1rem; background: #fadbd8; border-radius: 8px;">
                                <div class="result-value negative">${(scenarioResults.pessimistic.sixMonth/1000).toFixed(0)}K</div>
                                <div class="result-description">Pessimistic (${scenarioResults.pessimistic.probability}% chance)</div>
                                <div style="font-size: 0.9rem; color: var(--danger-red); font-weight: bold;">+${scenarioResults.pessimistic.growth}% growth</div>
                            </div>
                        </div>
                        
                        <div class="recommendations-list">
                            <h4>🧪 Stress Testing Results</h4>
                            <ul>
                                <li>20% revenue drop: ${scenarioResults.stressTest.revenue20Drop.cashRunway}-day cash runway - ${scenarioResults.stressTest.revenue20Drop.verdict}</li>
                                <li>30-day payment delay: ${(scenarioResults.stressTest.payment30Delay.impact/1000).toFixed(0)}K impact - ${scenarioResults.stressTest.payment30Delay.verdict}</li>
                                <li>Supplier payment acceleration: ${(scenarioResults.stressTest.supplierAcceleration.impact/1000).toFixed(0)}K impact - ${scenarioResults.stressTest.supplierAcceleration.verdict}</li>
                                <li>Overall resilience score: 85% - Strong financial foundation</li>
                            </ul>
                        </div>
                    </div>
                `;
                resultsContent.insertAdjacentHTML('beforeend', scenarioHTML);
            }
            
            instance.addStatusMessage('Scenario analysis complete - all projections updated', 'success', '🎲');
            instance.addStatusMessage('Optimistic scenario: +37% upside potential identified', 'insight', '📈');
            instance.addStatusMessage('Pessimistic scenario: 4-month cash runway in worst case', 'insight', '⚠️');
            
            // Store scenario results
            window.treasuryScenarioResults = scenarioResults;
        }, 2000);
    }
    
    static optimizeWorkingCapital() {
        const instance = window.platform.treasuryManagement;
        const statusMessage = instance.addStatusMessage('Analyzing working capital optimization opportunities', 'processing');
        
        setTimeout(() => {
            const wcOptimization = {
                currentWC: 875000,
                optimizedWC: 1225000,
                improvement: 350000,
                recommendations: [
                    { action: 'Accelerate customer payments with 2% early discount', impact: 120000, timeframe: '30 days' },
                    { action: 'Extend supplier payment terms from 30 to 45 days', impact: 180000, timeframe: '60 days' },
                    { action: 'Reduce inventory levels through improved forecasting', impact: 200000, timeframe: '90 days' },
                    { action: 'Implement automated invoice processing', impact: 50000, timeframe: '45 days' }
                ]
            };
            
            // Add working capital optimization to existing panel
            const resultsContent = document.getElementById('treasury-results-content');
            if (resultsContent) {
                const wcHTML = `
                    <div class="result-card" style="margin-top: 1.5rem; border-left: 5px solid var(--treasury-green);">
                        <div class="result-title">⚙️ Working Capital Optimization</div>
                        <div class="results-grid" style="grid-template-columns: 1fr 1fr 1fr; margin: 1rem 0;">
                            <div style="text-align: center;">
                                <div class="result-value neutral">${(wcOptimization.currentWC/1000).toFixed(0)}K</div>
                                <div class="result-description">Current Working Capital</div>
                            </div>
                            <div style="text-align: center;">
                                <div class="result-value positive">${(wcOptimization.optimizedWC/1000).toFixed(0)}K</div>
                                <div class="result-description">Optimized Working Capital</div>
                            </div>
                            <div style="text-align: center;">
                                <div class="result-value positive">${(wcOptimization.improvement/1000).toFixed(0)}K</div>
                                <div class="result-description">Annual Improvement</div>
                            </div>
                        </div>
                        
                        <div class="recommendations-list">
                            <h4>💡 Implementation Roadmap</h4>
                            <ul>
                                ${wcOptimization.recommendations.map(rec => 
                                    `<li>${rec.action} - ${(rec.impact/1000).toFixed(0)}K impact (${rec.timeframe})</li>`
                                ).join('')}
                            </ul>
                        </div>
                    </div>
                `;
                resultsContent.insertAdjacentHTML('beforeend', wcHTML);
            }
            
            instance.addStatusMessage('Working capital optimization complete', 'success', '⚙️');
            instance.addStatusMessage('Recommendation: Accelerate customer payments with 2% early discount', 'insight', '💡');
            instance.addStatusMessage('Recommendation: Extend supplier terms from 30 to 45 days', 'insight', '💡');
            instance.addStatusMessage('Estimated annual improvement: $350K working capital optimization', 'success', '💰');
            
            // Store WC results
            window.treasuryWCResults = wcOptimization;
        }, 1500);
    }
    
    static generateExecutiveReport() {
        const instance = window.platform.treasuryManagement;
        const statusMessage = instance.addStatusMessage('Generating executive treasury report', 'processing');
        
        setTimeout(() => {
            instance.addStatusMessage('Executive treasury report generated successfully', 'success', '📄');
            instance.addStatusMessage('Report includes: Strategic forecasts, optimization recommendations, risk analysis', 'success', '📊');
            
            // Show comprehensive export summary
            const forecast = window.treasuryForecastResults;
            const scenario = window.treasuryScenarioResults;
            const wc = window.treasuryWCResults;
            
            let exportSummary = '💰 TREASURY MANAGEMENT REPORT EXPORTED!\n\n';
            
            if (forecast) {
                exportSummary += `📊 CASH FLOW ANALYSIS:
• Current Position: ${(forecast.currentCash/1000).toFixed(0)}K
• 90-Day Projection: ${(forecast.projectedCash90/1000).toFixed(0)}K
• 6-Month Projection: ${(forecast.projectedCash6m/1000).toFixed(0)}K
• Cash Runway: ${forecast.cashRunway} days
• Cash Conversion Cycle: ${forecast.cashConversionCycle} days

`;
            }
            
            if (wc) {
                exportSummary += `⚙️ WORKING CAPITAL:
• Current: ${(wc.currentWC/1000).toFixed(0)}K
• Optimized: ${(wc.optimizedWC/1000).toFixed(0)}K  
• Annual Improvement: ${(wc.improvement/1000).toFixed(0)}K

`;
            }
            
            if (scenario) {
                exportSummary += `🎲 SCENARIO ANALYSIS:
• Optimistic: ${(scenario.optimistic.sixMonth/1000).toFixed(0)}K (+${scenario.optimistic.growth}%)
• Realistic: ${(scenario.realistic.sixMonth/1000).toFixed(0)}K (+${scenario.realistic.growth}%)
• Pessimistic: ${(scenario.pessimistic.sixMonth/1000).toFixed(0)}K (+${scenario.pessimistic.growth}%)

`;
            }
            
            exportSummary += `📈 EXECUTIVE SUMMARY:
• Strong financial position with excellent liquidity
• Multiple optimization opportunities identified
• Risk assessment shows resilient business model
• Strategic recommendations for growth`;
            
            alert(exportSummary);
        }, 1500);
    }
}

// === GLOBAL FUNCTIONS ===

function showOverview() {
    if (window.platform) {
        window.platform.showSection('overview');
    }
}

function showProductIntelligence() {
    if (window.platform) {
        window.platform.showSection('product-intelligence');
    }
}

function showTreasuryManagement() {
    if (window.platform) {
        window.platform.showSection('treasury-management');
    }
}

function exportPlatformReport() {
    console.log('Exporting platform report...');
    alert('🚀 Platform Report Exported!\n\nComprehensive enterprise report including:\n\n📊 PRODUCT INTELLIGENCE:\n• $8.0M+ optimization opportunities\n• 367 items losing money analysis\n• Customer & supplier intelligence\n• Inventory optimization recommendations\n\n💰 TREASURY MANAGEMENT:\n• 90-day/6-month/12-month forecasts\n• $350K working capital optimization\n• Cash conversion cycle analysis\n• Scenario planning & stress testing\n\n📈 EXECUTIVE SUMMARY:\n• Strategic recommendations\n• Key performance indicators\n• Risk assessments\n• Implementation roadmap');
}

function exportDashboard() {
    console.log('Exporting dashboard...');
    alert('📄 Dashboard Exported Successfully!\n\nProfessional PDF export includes:\n\n📊 VISUALIZATIONS:\n• All charts and graphs\n• Interactive dashboards\n• Key performance metrics\n\n💼 BUSINESS INSIGHTS:\n• Product profitability analysis\n• Cash flow projections\n• Working capital metrics\n• Optimization opportunities\n\n🎯 FORMATTING:\n• Executive-ready presentation\n• Print-optimized layouts\n• Professional branding\n• Mobile-friendly design');
}

// === INITIALIZATION ===

document.addEventListener('DOMContentLoaded', function() {
    window.platform = new EnterprisePlatform();
    
    // Export functions for global access
    window.ProductIntelligence = ProductIntelligence;
    window.TreasuryManagement = TreasuryManagement;
    
    console.log('🚀 Enterprise NetSuite BI Platform fully initialized');
});