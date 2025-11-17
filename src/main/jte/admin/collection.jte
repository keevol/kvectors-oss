@import io.vertx.core.json.*

@param JsonObject viewModel

<%-- declare a local variable with java syntax, so ; is a must :-( --%>
!{var name = viewModel.getString("collectionName");}
!{var colType = viewModel.getString("collectionType");}
!{var subtitle = "Vector Collection ▻ " + name + "("+colType+")";}

@template.admin.layout.layout(title = "Vector Collection Detail", subtitle = subtitle, json = viewModel, content = @`
    <div class="flex-1 flex flex-col overflow-hidden lg:ml-0">
        <!-- Main Content Area -->
        <main class="flex-1 overflow-y-auto p-4 lg:p-6 bg-slate-800">
            <!-- Action Buttons -->
            <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 space-y-3 sm:space-y-0">
                <div class="flex items-center">
                    <i class="fas fa-database text-cyan-400 text-xl mr-3"></i>
                    <div>
                        <h3 class="text-lg font-medium text-cyan-300">向量集合 ${name}</h3>
                        <p class="text-sm text-slate-400">125 total records</p>
                    </div>
                </div>
                <div class="flex flex-wrap gap-3">
                    <a href="/collection/vector/add?collection=${name}"
                       class="bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-700 hover:to-blue-700 text-white px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 glow-effect">
                        <i class="fas fa-plus mr-2"></i>Add Vector
                    </a>
                    <a href="/collection/index/build?collection=${name}"
                       class="bg-gradient-to-r from-yellow-600 to-orange-600 hover:from-yellow-700 hover:to-orange-700 text-white px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200">
                        <i class="fas fa-edit mr-2"></i>Build New Full Index
                    </a>
                    <a href="#" hx-post="/collection/index/build/history?collection=${name}" hx-target="#buildHistory" hx-swap="innerHTML"
                       class="bg-gradient-to-r from-yellow-600 to-orange-600 hover:from-yellow-700 hover:to-orange-700 text-white px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200">
                        <i class="fas  fa-list mr-2"></i>查看索引构建日志（history）
                    </a>
                    <a href="#" hx-post="/collection/drop?collection=${name}" hx-swap="none" hx-confirm="确定要整个的删除（drop）当前向量集合吗？"
                       class="bg-gradient-to-r from-red-600 to-blue-600 hover:from-yellow-700 hover:to-orange-700 text-white px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200">
                        <i class="fas fa-unlink mr-2"></i>删除集合(⚠️)
                    </a>
                </div>
            </div>


            <!-- Search and Filter -->
            <div class="cyber-card p-4 rounded-xl shadow-xl mb-6">
                <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between space-y-3 lg:space-y-0">
                    <form class="w-full lg:flex lg:space-x-4 lg:items-center">
                        <input type="text" id="collectionName" name="collectionName" value="${name}" hidden="hidden"/>
                        <!-- Text Search Field -->
                        <div class="lg:w-1/2 mb-3 lg:mb-0 flex flex-col sm:flex-row sm:items-center">
                            <label for="queryVector"
                                   class="text-sm font-medium text-blue-300 whitespace-nowrap mb-1 sm:mb-0 sm:mr-2 w-32">搜索向量(vector)</label>
                            <div class="relative flex-1">
                                <input type="text" id="queryVector" name="queryVector" placeholder="Vector to query"
                                       class="tech-input w-full pl-10 pr-4 py-3 rounded-lg focus:outline-none transition-all duration-200">
                                <div class="absolute inset-y-0 left-0 pl-3 flex items-center">
                                    <i class="fas fa-search text-blue-400"></i>
                                </div>
                            </div>
                        </div>

                        <!-- Number Input Field -->
                        <div class="lg:w-1/4 mb-3 lg:mb-0 flex flex-col sm:flex-row sm:items-center">
                            <label for="topK"
                                   class="text-sm font-medium text-blue-300 whitespace-nowrap mb-1 sm:mb-0 sm:mr-2 w-32">结果数量(TopK)</label>
                            <div class="relative flex-1">
                                <input type="number" id="topK" name="topK" placeholder="topK" value="11"
                                       class="tech-input w-full px-4 py-3 rounded-lg focus:outline-none transition-all duration-200">
                            </div>
                        </div>

                        <!-- Float Input Field -->
                        <div class="lg:w-1/4 mb-3 lg:mb-0 flex flex-col sm:flex-row sm:items-center">
                            <label for="score"
                                   class="text-sm font-medium text-blue-300 whitespace-nowrap mb-1 sm:mb-0 sm:mr-2 w-32">相似度标准(score)</label>
                            <div class="relative flex-1">
                                <input type="number" id="score" name="score" step="0.01" placeholder="score.."
                                       value="0.8"
                                       class="tech-input w-full px-4 py-3 rounded-lg focus:outline-none transition-all duration-200">
                            </div>
                        </div>

                        <div class="flex flex-col sm:flex-row items-center space-y-3 sm:space-y-0 sm:space-x-3 lg:flex-shrink-0 lg:ml-4">
                            <button class="bg-gradient-to-r from-slate-600 to-slate-700 hover:from-slate-700 hover:to-slate-800 text-slate-200 px-4 py-3 rounded-lg text-sm transition-all duration-200 border border-slate-500 w-full sm:w-auto"
                                    hx-post="/collection/vector/query"
                                    hx-target="#results"
                                    hx-swap="innerHTML"
                            >
                                <i class="	fas fa-caret-right mr-2"></i>搜索相似向量
                            </button>
                        </div>
                    </form>


                </div>
            </div>

            <!-- Records Zone -->
            <div class="shadow-2xl rounded-xl overflow-hidden">
                <div class="overflow-x-auto" id="results">

                </div>
            </div>
            <div class="shadow-2xl rounded-xl overflow-hidden">
                <div class="overflow-x-auto" id="buildHistory">

                </div>
            </div>
        </main>
`)
