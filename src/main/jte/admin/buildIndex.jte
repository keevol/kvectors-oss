@import io.vertx.core.json.*

@param JsonObject viewModel

!{var name = viewModel.getString("collectionName");}

@template.admin.layout.layout(title= "构建全量索引", subtitle = "构建全量索引", json = viewModel, content = @`

    <!-- Main Content Area -->
    <main class="flex-1 overflow-y-auto p-4 lg:p-6 bg-slate-800">
        <div class="max-w-4xl mx-auto">
            <!-- Action Buttons -->
            <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 space-y-3 sm:space-y-0">
                <div class="flex items-center">
                    <i class="fas fa-database text-green-400 text-xl mr-3"></i>
                    <div>
                        <h3 class="text-lg font-medium text-green-300">构建全量索引-${name}</h3>
                    </div>
                </div>
                <div class="flex flex-wrap gap-3">
                    <a href="#" onclick="history.back();return false; "
                       class="bg-gradient-to-r from-slate-600 to-slate-700 hover:from-slate-700 hover:to-slate-800 text-slate-200 px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 border border-slate-500">
                        <i class="fas fa-arrow-left mr-2"></i>返回
                    </a>
                </div>
            </div>

            <div class="cyber-card shadow-2xl rounded-xl overflow-hidden">
                <form class="p-6 space-y-6" method="post" hx-post="/collection/index/build?collection=${name}"
                      hx-swap="none">

                    <div>
                        <label for="enable-index-right-now" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-icons mr-2"></i>构建完成后立即启用
                        </label>
                        <select id="enable-index-right-now" name="enable-index-right-now"
                                class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                            <option value="yes">立即启用</option>
                            <option value="no">不启用</option>
                        </select>
                    </div>
                    <div>
                        <label for="webhook" class="block text-sm font-medium text-blue-300 mb-2">
                            <i class="fas fa-database mr-2"></i>Webhook
                        </label>
                        <input type="text" id="webhook" name="webhook"
                               class="tech-input block w-full rounded-lg px-4 py-3 text-sm transition-all duration-200 focus:outline-none">
                        <p class="mt-2 text-sm text-slate-400">索引构建完成后的通知地址</p>
                    </div>

                    <!-- Submit Buttons -->
                    <div class="flex flex-col sm:flex-row justify-end space-y-3 sm:space-y-0 sm:space-x-3 pt-4">
                        <a href="#"
                           class="bg-slate-600 hover:bg-slate-500 text-slate-200 px-6 py-3 rounded-lg text-sm font-medium transition-all duration-200 border border-slate-500 text-center"
                           onclick="history.back();return false; ">
                            取消操作
                        </a>
                        <button type="submit"
                                class="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white px-6 py-3 rounded-lg text-sm font-medium transition-all duration-200 glow-effect">
                            <i class="fas fa-plus mr-2"></i>马上构建！
                        </button>
                    </div>
                </form>
            </div>
        </div>
    </main>

`)
